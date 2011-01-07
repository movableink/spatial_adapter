require 'spatial_adapter'
require 'active_record/connection_adapters/ibm_db_adapter'

ActiveRecord::ConnectionAdapters::IBM_DBAdapter.class_eval do
  include SpatialAdapter
  
  def db2_gse_functions_present?
    begin
      function_count = select_one("SELECT COUNT(*) AS FUNC_COUNT FROM SYSCAT.FUNCTIONS WHERE FUNCSCHEMA = 'DB2GSE'")[:FUNC_COUNT]
      function_count.to_i > 0
    rescue ActiveRecord::StatementInvalid
      false
    end
  end
  
  def spatial?
    db2_gse_functions_present?
  end
  
  def supports_geographic?
    db2_gse_functions_present?
  end
  
  alias :original_native_database_types :native_database_types
  def native_database_types
    original_native_database_types.merge!(geometry_data_types)
  end

  alias :original_quote :quote
  #Redefines the quote method to add behaviour for when a Geometry is encountered
  def quote(value, column = nil)
    
    if value.kind_of?(GeoRuby::SimpleFeatures::Point)
      "DB2GSE.ST_PointFromWKB(CAST(x'#{value.as_hex_wkb}' AS BLOB), 1)"
    elsif value.kind_of?(GeoRuby::SimpleFeatures::LineString)
      "DB2GSE.ST_LineFromWKB(CAST(x'#{value.as_hex_wkb}' AS BLOB), 1)"
    elsif value.kind_of?(GeoRuby::SimpleFeatures::Polygon)
      "DB2GSE.ST_PolyFromWKB(CAST(x'#{value.as_hex_wkb}' AS BLOB), 1)"
    elsif value.kind_of?(GeoRuby::SimpleFeatures::MultiPoint)
      "DB2GSE.ST_MPointFromWKB(CAST(x'#{value.as_hex_wkb}' AS BLOB), 1)"
    elsif value.kind_of?(GeoRuby::SimpleFeatures::MultiPolygon)
      "DB2GSE.ST_MPolyFromWKB(CAST(x'#{value.as_hex_wkb}' AS BLOB), 1)"
    elsif value.kind_of?(GeoRuby::SimpleFeatures::MultiLineString)
      "DB2GSE.ST_MLineFromWKB(CAST(x'#{value.as_hex_wkb}' AS BLOB), 1)"
    elsif value.kind_of?(GeoRuby::SimpleFeatures::GeometryCollection)
      "DB2GSE.ST_GeomCollFromWKB(CAST(x'#{value.as_hex_wkb}' AS BLOB), 1)"
    else
      original_quote(value,column)
    end
  end
  
  # Returns an array of Column objects for the table specified by +table_name+
  def columns(table_name, name = nil)
    # to_s required because it may be a symbol.
    table_name = @servertype.set_case(table_name.to_s)
    # Checks if a blank table name has been given.
    # If so it returns an empty array
    return [] if table_name.strip.empty?
    # +columns+ will contain the resulting array
    columns = []
    # find any GeoSpatial Extender columns so they can be flagged later
    raw_geom_infos = column_spatial_info(table_name)
    # Statement required to access all the columns information
    stmt = IBM_DB.columns( @connection, nil, 
                               @servertype.set_case(@schema), 
                               @servertype.set_case(table_name) )
    if(stmt)
      begin
        # Fetches all the columns and assigns them to col.
        # +col+ is an hash with keys/value pairs for a column
        while col = IBM_DB.fetch_assoc(stmt)
          column_name = col["column_name"].downcase
          # Assigns the column default value.
          column_default_value = col["column_def"]
          # If there is no default value, it assigns NIL
          column_default_value = nil if (column_default_value && column_default_value.upcase == 'NULL')
          # Removes single quotes from the default value
          column_default_value.gsub!(/^'(.*)'$/, '\1') unless column_default_value.nil?
          # Assigns the column type
          column_type = col["type_name"].downcase
          # Assigns the field length (size) for the column
          column_length = col["column_size"]
          column_scale = col["decimal_digits"]
          # The initializer of the class Column, requires the +column_length+ to be declared 
          # between brackets after the datatype(e.g VARCHAR(50)) for :string and :text types. 
          # If it's a "for bit data" field it does a subsitution in place, if not
          # it appends the (column_length) string on the supported data types
          unless column_length.nil? || 
                 column_length == '' || 
                 column_type.sub!(/ \(\) for bit data/i,"(#{column_length}) FOR BIT DATA") || 
                 !column_type =~ /char|lob|graphic/i
            if column_type =~ /decimal/i
              column_type << "(#{column_length},#{column_scale})"
            elsif column_type =~ /smallint|integer|double|date|time|timestamp|xml|bigint/i
              column_type << ""  # override native limits incompatible with table create
            else
              column_type << "(#{column_length})"
            end
          end
          # col["NULLABLE"] is 1 if the field is nullable, 0 if not.
          column_nullable = (col["nullable"] == 1) ? true : false
          # Make sure the hidden column (db2_generated_rowid_for_lobs) in DB2 z/OS isn't added to the list
          if !(column_name =~ /db2_generated_rowid_for_lobs/i)
            # Pushes into the array the *IBM_DBColumn* object, created by passing to the initializer
            # +column_name+, +default_value+, +column_type+ and +column_nullable+.
            # if this column is of a type from GeoSpatial Extender, add as a Spatial Column
            if raw_geom_infos.key?(column_name.upcase)
              g_col = raw_geom_infos[column_name.upcase]
              columns << ActiveRecord::ConnectionAdapters::SpatialIBM_DBColumn.new(column_name, column_default_value, g_col.type, column_nullable, g_col.srid, g_col.with_z, g_col.with_m)
            else
              columns << ActiveRecord::ConnectionAdapters::IBM_DBColumn.new(column_name, column_default_value, column_type, column_nullable)
            end
          end
        end
      rescue StandardError => fetch_error # Handle driver fetch errors
        error_msg = IBM_DB.getErrormsg(stmt, IBM_DB::DB_STMT )
        if error_msg && !error_msg.empty?
          raise "Failed to retrieve column metadata during fetch: #{error_msg}"
        else
          error_msg = "An unexpected error occurred during retrieval of column metadata"
          error_msg = error_msg + ": #{fetch_error.message}" if !fetch_error.message.empty?
          raise error_msg
        end
      ensure  # Free resources associated with the statement
        IBM_DB.free_stmt(stmt) if stmt
      end
    else  # Handle driver execution errors
      error_msg = IBM_DB.getErrormsg(@connection, IBM_DB::DB_CONN )
      if error_msg && !error_msg.empty?
        raise "Failed to retrieve column metadata due to error: #{error_msg}"
      else
        raise StandardError.new('An unexpected error occurred during retrieval of columns metadata')
      end
    end
    # Returns the columns array
    return columns
  end

  def create_table(table_name, options = {})
    # Using the subclassed table definition
    table_definition = ActiveRecord::ConnectionAdapters::IBM_DBTableDefinition.new(self)
    table_definition.primary_key(options[:primary_key] || ActiveRecord::Base.get_primary_key(table_name.to_s.singularize)) unless options[:id] == false

    yield table_definition if block_given?

    if options[:force] && table_exists?(table_name)
      drop_table(table_name, options)
    end

    create_sql = "CREATE#{' TEMPORARY' if options[:temporary]} TABLE "
    create_sql << "#{quote_table_name(table_name)} ("
    create_sql << table_definition.to_sql
    create_sql << ") #{options[:options]}"

    # This is the additional portion for PostGIS
    unless table_definition.geom_columns.nil?
      table_definition.geom_columns.each do |geom_column|
        geom_column.table_name = table_name
        create_sql << "; " + geom_column.to_sql
      end
    end

    execute create_sql
  end

  alias :original_remove_column :remove_column
  def remove_column(table_name, *column_names)
    column_names = column_names.flatten
    columns(table_name).each do |col|
      if column_names.include?(col.name.to_sym)
        # Geometry columns have to be removed using DropGeometryColumn
        if col.is_a?(SpatialColumn) && col.spatial? && !col.geographic?
          execute "SELECT DropGeometryColumn('#{table_name}','#{col.name}')"
        else
          original_remove_column(table_name, col.name)
        end
      end
    end
  end
  
  alias :original_add_column :add_column
  def add_column(table_name, column_name, type, options = {})
    unless geometry_data_types[type].nil?
      geom_column = ActiveRecord::ConnectionAdapters::IBM_DBColumnDefinition.new(self, column_name, type, nil, nil, options[:null], options[:srid] || -1 , options[:with_z] || false , options[:with_m] || false, options[:geographic] || false)
      if geom_column.geographic
        default = options[:default]
        notnull = options[:null] == false
        
        execute("ALTER TABLE #{quote_table_name(table_name)} ADD COLUMN #{geom_column.to_sql}")

        change_column_default(table_name, column_name, default) if options_include_default?(options)
        change_column_null(table_name, column_name, false, default) if notnull
      else
        geom_column.table_name = table_name
        execute geom_column.to_sql
      end
    else
      original_add_column(table_name, column_name, type, options)
    end
  end

  # Adds an index to a column.
  def add_index(table_name, column_name, options = {})
    column_names = Array(column_name)
    index_name   = index_name(table_name, :column => column_names)

    if Hash === options # legacy support, since this param was a string
      index_type = options[:unique] ? "UNIQUE" : ""
      index_name = options[:name] || index_name
      index_method = options[:spatial] ? 'USING GIST' : ""
    else
      index_type = options
    end
    quoted_column_names = column_names.map { |e| quote_column_name(e) }.join(", ")
    execute "CREATE #{index_type} INDEX #{quote_column_name(index_name)} ON #{quote_table_name(table_name)} #{index_method} (#{quoted_column_names})"
  end

  # Returns the list of all indexes for a table.
  #
  # This is a full replacement for the ActiveRecord method and as a result
  # has a higher probability of breaking in future releases.
  def indexes(table_name, name = nil)
     schemas = schema_search_path.split(/,/).map { |p| quote(p) }.join(',')
     
     # Changed from upstread: link to pg_am to grab the index type (e.g. "gist")
     result = select(<<-SQL, name)
       SELECT distinct i.relname, d.indisunique, d.indkey, t.oid, am.amname
         FROM pg_class t, pg_class i, pg_index d, pg_attribute a, pg_am am
       WHERE i.relkind = 'i'
         AND d.indexrelid = i.oid
         AND d.indisprimary = 'f'
         AND t.oid = d.indrelid
         AND t.relname = '#{table_name}'
         AND i.relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname IN (#{schemas}) )
         AND i.relam = am.oid
         AND a.attrelid = t.oid
      ORDER BY i.relname
    SQL


    indexes = []

    indexes = result.map do |row|
      index_name = row[0]
      unique = row[1] == 't'
      indkey = row[2].split(" ")
      oid = row[3]
      indtype = row[4]

      # Changed from upstream: need to get the column types to test for spatial indexes
      columns = select(<<-SQL, "Columns for index #{row[0]} on #{table_name}").inject({}) {|attlist, r| attlist[r[1]] = [r[0], r[2]]; attlist}
      SELECT a.attname, a.attnum, t.typname
      FROM pg_attribute a, pg_type t
      WHERE a.attrelid = #{oid}
      AND a.attnum IN (#{indkey.join(",")})
      AND a.atttypid = t.oid
      SQL

      # Only GiST indexes on spatial columns denote a spatial index
      spatial = indtype == 'gist' && columns.size == 1 && (columns.values.first[1] == 'geometry' || columns.values.first[1] == 'geography')

      column_names = indkey.map {|attnum| columns[attnum] ? columns[attnum][0] : nil }
      ActiveRecord::ConnectionAdapters::IndexDefinition.new(table_name, index_name, unique, column_names, spatial)
    end

    indexes
  end
  
  def column_spatial_info(table_name)
    constr = select("SELECT * FROM DB2GSE.ST_GEOMETRY_COLUMNS WHERE TABLE_NAME = '#{table_name.upcase}'")

    raw_geom_infos = {}
    constr.each do |column|
      raw_geom_infos[column['column_name']] ||= SpatialAdapter::RawGeomInfo.new
      raw_geom_infos[column['column_name']].type = column['type_name']
      raw_geom_infos[column['column_name']].srid = column['srs_id']
    end

    raw_geom_infos.each_value do |raw_geom_info|
      #check the presence of z and m
      raw_geom_info.convert!
    end

    raw_geom_infos
  end
end

module ActiveRecord
  module ConnectionAdapters
    class IBM_DBTableDefinition < TableDefinition
      attr_reader :geom_columns
      
      def column(name, type, options = {})
        unless (@base.geometry_data_types[type.to_sym].nil? or
                (options[:create_using_addgeometrycolumn] == false))

          column = self[name] || IBM_DBColumnDefinition.new(@base, name, type)
          column.null = options[:null]
          column.srid = options[:srid] || -1
          column.with_z = options[:with_z] || false 
          column.with_m = options[:with_m] || false
          column.geographic = options[:geographic] || false

          if column.geographic
            @columns << column unless @columns.include? column
          else
            # Hold this column for later
            @geom_columns ||= []
            @geom_columns << column
          end
          self
        else
          super(name, type, options)
        end
      end    
    end

    class IBM_DBColumnDefinition < ColumnDefinition
      attr_accessor :table_name
      attr_accessor :srid, :with_z, :with_m, :geographic
      attr_reader :spatial

      def initialize(base = nil, name = nil, type=nil, limit=nil, default=nil, null=nil, srid=-1, with_z=false, with_m=false, geographic=false)
        super(base, name, type, limit, default, null)
        @table_name = nil
        @spatial = true
        @srid = srid
        @with_z = with_z
        @with_m = with_m
        @geographic = geographic
      end
      
      def sql_type
        if geographic
          type_sql = base.geometry_data_types[type.to_sym][:name]
          type_sql += "Z" if with_z
          type_sql += "M" if with_m
          # SRID is not yet supported (defaults to 4326)
          #type_sql += ", #{srid}" if (srid && srid != -1)
          type_sql = "geography(#{type_sql})"
          type_sql
        else
          super
        end
      end
      
      def to_sql
        if spatial && !geographic
          type_sql = base.geometry_data_types[type.to_sym][:name]
          type_sql += "M" if with_m and !with_z
          if with_m and with_z
            dimension = 4 
          elsif with_m or with_z
            dimension = 3
          else
            dimension = 2
          end
        
          column_sql = "SELECT AddGeometryColumn('#{table_name}','#{name}',#{srid},'#{type_sql}',#{dimension})"
          column_sql += ";ALTER TABLE #{table_name} ALTER #{name} SET NOT NULL" if null == false
          column_sql
        else
          super
        end
      end
    end
  end
end

module ActiveRecord
  module ConnectionAdapters
    class SpatialIBM_DBColumn < IBM_DBColumn
      include SpatialAdapter::SpatialColumn

      def initialize(name, default, sql_type = nil, null = true, srid=-1, with_z=false, with_m=false, geographic = false)
        super(name, default, sql_type, null, srid, with_z, with_m)
        @geographic = geographic
      end

      def geographic?
        @geographic
      end
      
      # Transforms a string to a geometry. DB2 GSE returns a EWKB string.
      # Importantly: DB2 must be configured to return WKB by default from the db
      # SQL:  SET CURRENT DEFAULT TRANSFORM GROUP = ST_WellKnownBinary;
      def self.string_to_geometry(string)
        return string unless string.is_a?(String)
        GeoRuby::SimpleFeatures::Geometry.from_ewkb(string) rescue nil
      end
      
      private
      
      # Add detection of DB2GSE-specific geography columns
      def geometry_simplified_type(sql_type)
        case sql_type
          when /ST_POINT/i then :point
          when /ST_LINESTRING/i then :line_string
          when /ST_POLYGON/i then :polygon
          when /ST_GEOMCOLLECTION/i then :geometry_collection
          when /ST_MULTIPOINT/i then :multi_point
          when /ST_MULTIPOLYGON/i then :multi_polygon
          when /ST_MULTILINESTRING/i then :multi_line_string
        else
          super
        end
      end

      def self.extract_geography_params(sql_type)
        params = {
          :srid => 0,
          :with_z => false,
          :with_m => false
        }
        if sql_type =~ /geography(?:\((?:\w+?)(Z)?(M)?(?:,(\d+))?\))?/i
          params[:with_z] = $1 == 'Z'
          params[:with_m] = $2 == 'M'
          params[:srid]   = $3.to_i
        end
        params
      end
    end
  end  
end
