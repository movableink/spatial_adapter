module SpatialAdapter::Base::Mysql
  module SpatialColumn
    def string_to_geometry(string)
      return string unless string.is_a?(String)
      GeoRuby::SimpleFeatures::Geometry.from_ewkb(string[4..-1])
    rescue Exception
      nil
    end
  end
end
