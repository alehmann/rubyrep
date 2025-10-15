$LOAD_PATH.unshift File.dirname(__FILE__) + '/..'

require 'drb'

require 'rubyrep'
require 'forwardable'

require 'active_record/connection_adapters/abstract_adapter'

module RR

  # Enables the fetching of (potential large) result sets in chunks.
  class ResultFetcher

    # The current database ProxyConnection
    attr_accessor :connection

    # hash of select options as described under ProxyConnection#select_cursor
    attr_accessor :options

    # column_name => value hash of the last returned row
    attr_accessor :last_row

    # The current row set: an array of column_name => value hashes
    attr_accessor :rows

    # Index to the current row in rows
    attr_accessor :current_row_index

    # Creates a new fetcher.
    # * +connection+: the current ProxyConnection
    # * +options+: hash of select options as described under ProxyConnection#select_cursor
    def initialize(connection, options)
      self.connection = connection
      self.options = options.clone
    end

    # Returns +true+ if there are more rows to read.
    def next?
      connection.send(:with_base_connection_pool) do
        unless self.rows
          # Try to load some records

          if options[:query] and last_row != nil
            # A query was directly specified and all it's rows were returned
            # ==> Finished.
            return false
          end

          if options[:query]
            # If a query has been directly specified, just directly execute it
            query = options[:query]
          else
            # Otherwise build the query
            if last_row
              # There was a previous batch.
              # Next batch will start after the last returned row
              options.merge! :from => last_row, :exclude_starting_row => true
            end

            query = connection.table_select_query(options[:table], options)

            if options[:row_buffer_size]
              # Set the batch size
              query += " limit #{options[:row_buffer_size]}"
            end
          end

          self.rows = connection.select_all query

          ###############
          # Below approach can only be used starting with activerecord version 5.
          # And as of 2017-05-18 jruby (i.e. version 9.1.7.0) supports only up to activerecord version 4.2.
          ###############
          # result = connection.select_all(query)
          # column_types = result.column_types
          # columns = result.columns
          # identity_type = ActiveRecord::Type::Value.new
          # types = columns.map { |name| column_types.fetch(name, identity_type) }
          # self.rows = result.rows.map do |values|
          #   row = {}
          #   columns.zip(types, values).each do |name, type, value|
          #     row[name] = type.deserialize(value)
          #   end
          #   row
          # end

          self.current_row_index = 0
        end
        self.current_row_index < self.rows.length
      end
    end

    # Returns the row as a column => value hash and moves the cursor to the next row.
    def next_row
      connection.send(:with_base_connection_pool) do
        raise("no more rows available") unless next?
        self.last_row = self.rows[self.current_row_index]
        self.current_row_index += 1

        if self.current_row_index == self.rows.length
          self.rows = nil
        end

        self.last_row
      end
    end

    # Frees up all ressources
    def clear
      self.rows = nil
    end
  end

  # This class represents a remote activerecord database connection.
  # Normally created by DatabaseProxy
  class ProxyConnection
    # Ensure that the proxy object always stays on server side and only remote
    # references are returned to the client.
    include DRbUndumped

    extend Forwardable
    
    # The database connection
    attr_accessor :connection

    # A hash as described by ActiveRecord::Base#establish_connection
    attr_accessor :config
    
    # Forward certain methods to the proxied database connection
    def_delegators :connection,
      :columns, :quote_column_name,
      :quote_table_name, :execute,
      :select_one, :select_all, :tables, :update, :delete,
      :begin_db_transaction, :rollback_db_transaction, :commit_db_transaction,
      :referenced_tables,
      :create_or_replace_replication_trigger_function,
      :create_replication_trigger, :drop_replication_trigger, :replication_trigger_exists?,
      :sequence_values, :update_sequences, :clear_sequence_setup,
      :drop_table, :add_big_primary_key, :add_column, :remove_column

    # Caching the primary keys. This is a hash with
    #   * key: table name
    #   * value: array of primary key names
    attr_accessor :primary_key_names_cache
    
    # Hash to register cursors.
    # Purpose:
    #   Objects only referenced remotely via DRb can be garbage collected.
    #   We register them in this hash to protect them from unintended garbage collection.
    attr_accessor :cursors
    
    # 2-level Hash of table_name => column_name => Column objects.
    attr_accessor :table_columns
    
    # Hash of table_name => array of column names pairs.
    attr_accessor :table_column_names

    # A hash of manually overwritten primary keys:
    # * key: table_name
    # * value: array of primary key names
    attr_accessor :manual_primary_keys
    
    # Executes the provided block with ActiveRecord::Base temporarily exposing the
    # current connection pool.
    #
    # @yield executes while the fallback pool is registered
    # @return [Object] the block result
    def with_base_connection_pool
      pool = connection.respond_to?(:pool) ? connection.pool : nil
      ConnectionExtenders.with_base_connection_pool(pool) { yield }
    end
    private :with_base_connection_pool

    WRAPPED_DELEGATES = [
      :columns, :quote_column_name, :quote_table_name, :execute,
      :select_one, :select_all, :tables, :update, :delete,
      :begin_db_transaction, :rollback_db_transaction, :commit_db_transaction,
      :referenced_tables, :create_or_replace_replication_trigger_function,
      :create_replication_trigger, :drop_replication_trigger,
      :replication_trigger_exists?, :sequence_values, :update_sequences,
      :clear_sequence_setup, :drop_table, :add_big_primary_key,
      :add_column, :remove_column
    ].freeze

    WRAPPED_DELEGATES.each do |method_name|
      define_method(method_name) do |*args, **kwargs, &block|
        with_base_connection_pool do
          connection.public_send(method_name, *args, **kwargs, &block)
        end
      end
    end

    if defined?(JRUBY_VERSION)
      alias_method :select_one_without_rr_normalization, :select_one
      alias_method :select_all_without_rr_normalization, :select_all

      # Executes the query and applies JRuby-specific UTF-8 normalization to the returned row.
      #
      # @param args [Array] positional arguments forwarded to the original implementation
      # @param kwargs [Hash] keyword arguments forwarded to the original implementation
      # @return [Hash, nil] a row hash with normalized column identifiers or `nil` when no row exists
      # Normalizes result hashes so JDBC keeps UTF-8 identifiers intact (JRuby only).
      def select_one(*args, **kwargs)
        normalize_select_result(select_one_without_rr_normalization(*args, **kwargs))
      end

      # Executes the query and applies JRuby-specific UTF-8 normalization to the returned rows.
      #
      # @param args [Array] positional arguments forwarded to the original implementation
      # @param kwargs [Hash] keyword arguments forwarded to the original implementation
      # @return [Array<Hash>, ActiveRecord::Result] a normalized result containing UTF-8-safe identifiers
      def select_all(*args, **kwargs)
        normalize_select_result(select_all_without_rr_normalization(*args, **kwargs))
      end
    end

    # Returns an array of primary key names for the given +table_name+.
    # Caches the result for future calls. Allows manual overwrites through
    # the Configuration options +:primary_key_names+ or :+primary_key_only_limit+.
    #
    # Parameters:
    # * +table_name+: name of the table
    # * +options+: An option hash with the following valid options:
    #   * :+raw+: if +true+, than don't use manual overwrites and don't cache
    def primary_key_names(table_name, options = {})
      with_base_connection_pool do
        return connection.primary_key_names(table_name) if options[:raw]

        self.primary_key_names_cache ||= {}
        result = primary_key_names_cache[table_name]
        unless result
          result = manual_primary_keys[table_name] || connection.primary_key_names(table_name)
          primary_key_names_cache[table_name] = result
        end
        result
      end
    end
    
    # Creates a table
    # Call forwarded to ActiveRecord::ConnectionAdapters::SchemaStatements#create_table
    # Provides an empty block (to prevent DRB from calling back the client)
    def create_table(*params)
      connection.create_table(*params) {}
    end

    # Returns a Hash of currently registerred cursors
    def cursors
      @cursors ||= {}
    end
    
    # Store a cursor in the register to protect it from the garbage collector.
    def save_cursor(cursor)
      cursors[cursor] = cursor
    end

    # Returns a cusor as produced by the #select_cursor method of the connection
    # extenders.
    #
    # Two modes of operation: Either
    # * execute the specified query (takes precedense) OR
    # * first build the query based on options forwarded to #table_select_query
    # +options+ is a hash with
    # * :+query+: executes the given query
    # * further options as taken by #table_select_query to build the query
    # * :+row_buffer_size+:
    #   Integer controlling how many rows a read into memory at one time.
    def select_cursor(options)
      with_base_connection_pool do
        cursor = ResultFetcher.new(self, options)
        cursor = TypeCastingCursor.new(self, options[:table], cursor)
        cursor
      end
    end

    # Reads the designated record from the database.
    # Refer to #select_cursor for details parameter description.
    # Returns the first matching row (column_name => value hash or +nil+).
    def select_record(options)
      with_base_connection_pool do
        cursor = select_cursor({:row_buffer_size => 1}.merge(options))
        row = cursor.next? ? cursor.next_row : nil
        cursor.clear
        row
      end
    end
    
    # Reads the designated records from the database.
    # Refer to #select_cursor for details parameter description.
    # Returns an array of matching rows (column_name => value hashes).
    def select_records(options)
      with_base_connection_pool do
        cursor = select_cursor(options)
        rows = []
        while cursor.next?
          rows << cursor.next_row
        end
        cursor.clear
        rows
      end
    end

    # Create a session on the proxy side according to provided configuration hash.
    # +config+ is a hash as described by ActiveRecord::Base#establish_connection
    def initialize(config)
      self.connection = ConnectionExtenders.db_connect config
      self.config = config
      self.manual_primary_keys = {}
    end

    # Destroys the session
    def destroy
      cursors.each_key do |cursor|
        cursor.destroy
      end
      cursors.clear

      if connection.log_subscriber
        ActiveSupport::Notifications.notifier.unsubscribe connection.log_subscriber
        connection.log_subscriber = nil
      end

      self.connection.disconnect!
    end
    
    # Quotes the given value. It is assumed that the value belongs to the specified column name and table name.
    # Caches the column objects for higher speed.
    def quote_value(table, column, value)
      with_base_connection_pool do
        self.table_columns ||= {}
        unless table_columns.include? table
          table_columns[table] = {}
          columns(table).each {|c| table_columns[table][c.name] = c}
        end
        connection.column_aware_quote value, table_columns[table][column.to_s]
      end
    end
    
    # Create a cursor for the given table.
    #   * +cursor_class+: should specify the Cursor class (e. g. ProxyBlockCursor or ProxyRowCursor).
    #   * +table+: name of the table 
    #   * +options+: An option hash that is used to construct the SQL query. See ProxyCursor#construct_query for details.
    def create_cursor(cursor_class, table, options = {})
      cursor = cursor_class.new self, table
      cursor.prepare_fetch options
      save_cursor cursor
      cursor
    end
    
    # Destroys the provided cursor and removes it from the register
    def destroy_cursor(cursor)
      cursor.destroy
      cursors.delete cursor
    end
    
    # Returns an array of column names of the given table name.
    # The array is ordered in the sequence as returned by the database.
    # The result is cached for higher speed.
    def column_names(table)
      with_base_connection_pool do
        self.table_column_names ||= {}
        unless table_column_names.include? table
          table_column_names[table] = columns(table).map {|c| c.name}
        end
        table_column_names[table]
      end
    end
  
    # Returns a list of quoted column names for the given +table+ as comma 
    # separated string.
    def quote_column_list(table)
      column_names(table).map do |column_name| 
        quote_column_name(column_name)
      end.join(', ')
    end
    private :quote_column_list
    
    # Returns a list of quoted primary key names for the given +table+ as comma
    # separated string.
    def quote_key_list(table)
      primary_key_names(table).map do |column_name| 
        quote_column_name(column_name)
      end.join(', ')
    end
    private :quote_key_list

    if defined?(JRUBY_VERSION)
      # Normalizes select results to repair mis-encoded identifiers returned by the
      # JDBC adapter when working with non-ASCII table or column names.
      #
      # @param result [Hash, Array<Hash>, ActiveRecord::Result, nil] raw driver response
      # @return [Hash, Array<Hash>, ActiveRecord::Result, nil] normalized result
      def normalize_select_result(result)
        case result
        when Hash
          normalize_result_row(result)
        when Array
          result.each { |row| normalize_result_row(row) }
        else
          normalize_active_record_result(result)
        end
      end
      private :normalize_select_result

      # Normalizes an `ActiveRecord::Result` instance by fixing all stored row hashes.
      #
      # @param result [ActiveRecord::Result] raw result produced by ActiveRecord
      # @return [ActiveRecord::Result] normalized result
      def normalize_active_record_result(result)
        return result unless defined?(ActiveRecord::Result) && result.is_a?(ActiveRecord::Result)

        if result.instance_variable_defined?(:@hash_rows)
          rows = result.instance_variable_get(:@hash_rows)
          rows&.each { |row| normalize_result_row(row) }
        else
          rows = result.to_a.map { |row| normalize_result_row(row) }
          result.instance_variable_set(:@hash_rows, rows)
        end

        result
      end
      private :normalize_active_record_result

      # Normalizes the keys of a result row hash in place.
      #
      # @param row [Hash] row returned by the JDBC adapter
      # @return [Hash] normalized row hash
      def normalize_result_row(row)
        return row unless row.is_a?(Hash)

        row.keys.each do |key|
          next unless key.is_a?(String)

          normalized = normalize_identifier(key)
          next if normalized.equal?(key) || normalized == key

          value = row.delete(key)
          row[normalized] = value
        end

        row
      end
      private :normalize_result_row

      # Restores UTF-8 identifiers that were double-encoded as ISO-8859-1 by the JDBC adapter.
      #
      # @param identifier [String] identifier to normalize
      # @return [String] identifier with corrected encoding, or the original string if conversion fails
      def normalize_identifier(identifier)
        identifier.encode('ISO-8859-1').force_encoding('UTF-8')
      rescue Encoding::InvalidByteSequenceError, Encoding::UndefinedConversionError
        identifier
      end
      private :normalize_identifier
    end

    
    # Generates an sql condition string for the given +table+ based on
    #   * +row+: a hash of primary key => value pairs designating the target row
    #   * +condition+: the type of sql condition (something like '>=' or '=', etc.)
    def row_condition(table, row, condition)
      query_part = ""
      query_part << ' (' << quote_key_list(table) << ') ' << condition
      query_part << ' (' << primary_key_names(table).map do |key|
        quote_value(table, key, row[key])
      end.join(', ') << ')'
      query_part
    end
    private :row_condition

    # Returns an SQL query string for the given +table+ based on the provided +options+.
    # +options+ is a hash that can contain any of the following:
    #   * :+from+: nil OR the hash of primary key => value pairs designating the start of the selection
    #   * :+exclude_starting_row+: if true, do not include the row specified by :+from+
    #   * :+to+: nil OR the hash of primary key => value pairs designating the end of the selection
    #   * :+row_keys+: an array of primary key => value hashes specify the target rows.
    def table_select_query(table, options = {})
      query = "select #{quote_column_list(table)}"
      query << " from #{quote_table_name(table)}"
      query << " where" if [:from, :to, :row_keys].any? {|key| options.include? key}
      first_condition = true
      if options[:from]
        first_condition = false
        matching_condition = options[:exclude_starting_row] ? '>' : '>='
        query << row_condition(table, options[:from], matching_condition)
      end
      if options[:to]
        query << ' and' unless first_condition
        first_condition = false
        query << row_condition(table, options[:to], '<=')
      end
      if options[:row_keys]
        query << ' and' unless first_condition
        if options[:row_keys].empty?
          query << ' false'
        else
          query << ' (' << quote_key_list(table) << ') in ('
          first_key = true
          options[:row_keys].each do |row|
            query << ', ' unless first_key
            first_key = false
            query << '(' << primary_key_names(table).map do |key|
              quote_value(table, key, row[key])
            end.join(', ') << ')'
          end
          query << ')'
        end
      end
      query << " order by #{quote_key_list(table)}"

      query
    end
    
    # Returns an SQL insert query for the given +table+ and +values+.
    # +values+ is a hash of column_name => value pairs.
    def table_insert_query(table, values)
      query = "insert into #{quote_table_name(table)}"
      query << '(' << values.keys.map do |column_name|
        quote_column_name(column_name)
      end.join(', ') << ') '
      query << 'values(' << values.map do |column_name, value|
        quote_value(table, column_name, value)
      end.join(', ') << ')'
      query
    end
    
    # Inserts the specified records into the named +table+.
    # +values+ is a hash of column_name => value pairs.
    def insert_record(table, values)
      execute table_insert_query(table, values)
    end
    
    # Returns an SQL update query.
    # * +table+: name of the target table
    # * +values+: a hash of column_name => value pairs
    # * +org_key+:
    #   A hash of column_name => value pairs. If +nil+, use the key specified by
    #   +values+ instead.
    def table_update_query(table, values, org_key = nil)
      org_key ||= values
      query = "update #{quote_table_name(table)} set "
      query << values.map do |column_name, value|
        "#{quote_column_name(column_name)} = #{quote_value(table, column_name, value)}"
      end.join(', ')
      query << " where (" << quote_key_list(table) << ") = ("
      query << primary_key_names(table).map do |key|
        quote_value(table, key, org_key[key])
      end.join(', ') << ")"
    end
    
    # Updates the specified records of the specified table.
    # * +table+: name of the target table
    # * +values+: a hash of column_name => value pairs.
    # * +org_key+:
    #   A hash of column_name => value pairs. If +nil+, use the key specified by
    #   +values+ instead.
    # Returns the number of modified records.
    def update_record(table, values, org_key = nil)
      update table_update_query(table, values, org_key)
    end

    # Returns an SQL delete query for the given +table+ and +values+
    # +values+ is a hash of column_name => value pairs. (Only the primary key
    # values will be used and must be included in the hash.)
    def table_delete_query(table, values)
      query = "delete from #{quote_table_name(table)}"
      query << " where (" << quote_key_list(table) << ") = ("
      query << primary_key_names(table).map do |key|
        quote_value(table, key, values[key])
      end.join(', ') << ")"
    end
    
    # Deletes the specified record from the named +table+.
    # +values+ is a hash of column_name => value pairs. (Only the primary key
    # values will be used and must be included in the hash.)
    # Returns the number of deleted records.
    def delete_record(table, values)
      delete table_delete_query(table, values)
    end
  end
end
