
require "base64"

module ActiveSupport
  module Cache
    class MongoStore
      ESCAPE_KEY_CHARS = /[\x00-\x20%\x7F-\xFF]/
      
      attr_reader :db_configuration, :mongo_connection      
      
      def initialize(opts = {})        
        default_capsize = (Rails.env == 'production') ? 512.megabytes : 256.megabytes
        user_config = YAML::load(ERB.new(IO.read(File.join(Rails.root, 'config/database.yml'))).result)[Rails.env]['mongo'] || {}
        @db_configuration = {
          'host' => 'localhost',
          'port' => 27017,
          'capsize' => default_capsize}.merge(user_config)
        @mongo_collection_name =  collection_name 
        @mongo_connection = Mongo::Connection.new(@db_configuration['host'], @db_configuration['port'], :auto_reconnect => true).db(@db_configuration['database'])
        unless @mongo_connection.collection_names.include?(@mongo_collection_name)
         @mongo_connection.create_collection(@mongo_collection_name, {:capped => true, :size => @db_configuration['capsize']})
        end
        @cache = @mongo_connection[@mongo_collection_name]
      end
  
      def fetch(key,value = nil,*)
        ret = read(key)
        return ret unless ret.nil?
        ret = yield if block_given?
        write(key,ret)
        ret
      end
      
      def delete(key, *)
        @cache.remove('_id' => escape_key(key))        
      end

      def write(key, value, *)        
          @cache.insert({ '_id' => escape_key(key), 'data' =>  serialize_entry(value) })
      end

      def stats
        @cache.stats
      end
      
      def clear(*)
        @cache.remove
      end

      def read(key)        
        ret = @cache.find_one('_id' => escape_key(key))
        deserialize_entry(ret['data']) rescue nil
      end
  #class << self
  #  attr_reader :mongo_collection_name, :mongo_connection

    # Drop the capped_collection and recreate it
    #    def reset_collection
    #  @mongo_connection[@mongo_collection_name].drop
    #  MongoStore.create_collection
    #end
    #end
  
    private
      def collection_name
          "#{Rails.env}_cache"
      end


      def serialize_entry(value)
        Marshal.dump(value)
      end

      def deserialize_entry(value)
        value && Marshal.load(value)
      end
      
      def escape_key(key)
        key = key.to_s.gsub(ESCAPE_KEY_CHARS){|match| "%#{match.getbyte(0).to_s(16).upcase}"}
        key = "#{key[0, 213]}:md5:#{Digest::MD5.hexdigest(key)}" if key.size > 250
        key
      end
  end
 end
end  

