module Sequel
  module Plugins
    # Calculate distance from a location based on latitude and longitude.
    #
    # Default latitude column is lat
    # Default longitude column is lng
    # Default distance column is distance
    # Default units is miles
    #   Options for units
    #     :miles => miles
    #     :kms   => kilometers
    #     :nms   => nautical miles
    #
    # If the Defaults are correct, simply include the plugin
    #
    # plugin :geosearch
    #
    # Or, you may provide options to specify different columns
    #
    # plugin :geosearch, 
    #   :latitude_column => :latitude, 
    #   :longitude_column => :longitude,
    #   :distance_column => :my_distance, 
    #   :distance_units => :nms
    #
    # When searching you must provide an origin.
    # The orgigin can be an array with [lat,lng]
    # or an Object that responds to the defined lat_col and lng_col
    #
    # Examples:
    #
    # Model.distance_from([34,-84])
    # Model.distance_from(Location.first)
    # Model.select(:name,address).distance_from(Location.first).
    #   order(:distance).limit(10)
    #
    # You can also use distance_boundary to limit searches to within a
    # certain range.  You must pass in the origin and the limit.
    #
    # Examples:
    # Find models that are <= 20 miles from the first Location
    # Model.distance_boundary(Location.first,20)
    module Geosearch
      class Error < Sequel::Error; end
      
      def self.apply(model, options={})
        model.plugin(:schema)
        return false unless model.table_exists?
        unless Sequel.const_defined?('MySQL') &&
               model.dataset.instance_of?(Sequel::MySQL::Dataset)
          puts "Geosearch only supports MySQL : disabled for #{model.to_s}"
          return false
        end
        
        # Earth radius based on the units required
        earth_radius = case options[:distance_units]
        when :miles, nil
          3963
        when :kms
          6378
        when :nms
          3444
        else
          raise(Error, "Units must be :miles, :kms, or :nms")
        end
        
        # Make sure distance column requested isn't defined in the table
        # Otherwise, create an instance method name the same so user can
        # call that method on the results ( eg: result.first.distance ) 
        dist_col = options[:distance_column] ||= :distance
        if model.columns.include?(dist_col)
          raise(Error, "#{dist_col} is already defined")
        end
        model.const_set('GEO_DIS_COL', dist_col)
        model.send(:define_method,dist_col) do
          self[dist_col]
        end
        
        # Setup some constants for the class to use when creating the
        # SELECT statement
        model.const_set('GEO_RADIUS',earth_radius)
        model.const_set('GEO_LAT_COL',options[:latitude_column] ||= :lat)
        model.const_set('GEO_LNG_COL',options[:longitude_column] ||= :lng)
        
        # Define the dataset method distance_from
        model.def_dataset_method(:distance_from) do |origin|
          # Get the distance_sql calculation
          _distance_sql = distance_sql(origin)
          # Sequel defines * to be the select options, if none have
          # been previously defined.  We will be defining a select
          # for the distance, so we need to add back the * if it
          # hasn't already been removed by a custom select
          select = (@opts[:select].nil? || @opts[:select].empty?) ?
            Sequel::Dataset::WILDCARD.lit : nil
            
          # Select more will add the select given to the already created
          # selects.  If none have been given it will be *, distance_column.
          # Otherwise, select what has previously been declared and add
          # the distance column to it
          select_more(
            *[select,_distance_sql.lit.as(self.model::GEO_DIS_COL)].compact
          )
        end
        
        # Define the dataset method distance_boundary
        model.def_dataset_method(:distance_boundary) do |origin,limit|
          # Get the distance_sql calculation
          _distance_sql = distance_sql(origin)
          
          # Filter the record
          filter(_distance_sql.lit <= limit)
        end
        
        model.def_dataset_method(:distance_sql) do |origin|
          lat, lng = nil, nil
          # If array [lat,lng]
          if origin.is_a?(Array) && origin.length == 2
            lat = origin[0]
            lng = origin[1]
          # Else check if the object responds to the defined columns
          elsif origin.respond_to?(self.model::GEO_LAT_COL) &&
                origin.respond_to?(self.model::GEO_LNG_COL)
            lat = origin.send(self.model::GEO_LAT_COL)
            lng = origin.send(self.model::GEO_LNG_COL)
          end
          
          if lat.blank? || lng.blank?
            raise(Error, "Cannot find Lat/Lng.")
          end

          # Convert the lat/lng to radians
          lat_radians=(lat/180) * Math::PI
          lng_radians=(lng/180) * Math::PI

          # Get the tablename associated with this model
          table = source_list(@opts[:from])

          # This qualifies the table/column (eg: monkeys.lat)
          lat_col = "#{table}.#{quote_identifier(self.model::GEO_LAT_COL)}"
          lng_col = "#{table}.#{quote_identifier(self.model::GEO_LNG_COL)}"

          # Generate the distance query
          distance_sql = <<EOD
(ACOS(
  least(1,COS(#{lat_radians})
  *COS(#{lng_radians})
  *COS(RADIANS(#{lat_col}))
  *COS(RADIANS(#{lng_col}))
  +COS(#{lat_radians})
  *SIN(#{lng_radians})
  *COS(RADIANS(#{lat_col}))
  *SIN(RADIANS(#{lng_col}))
  +SIN(#{lat_radians})
  *SIN(RADIANS(#{lat_col}))
))
*#{self.model::GEO_RADIUS})
EOD
          distance_sql
        end # end distance_sql
      end # apply
  
    end # Geosearch
  end # Plugins
end # Sequel
