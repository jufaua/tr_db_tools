module TrDbFetcher
  
  
  def self.select_db(database_name)
    ActiveRecord::Base.establish_connection database_name.to_sym
  end
  
  def self.revert_db
    ActiveRecord::Base.establish_connection Rails.env.to_sym
  end
  
  def self.execute(*string_queries)
    ActiveRecord::Base.transaction do
      string_queries.each do |query|
        ActiveRecord::Base.connection.execute(query)
      end
    end
  end
  
  def self.fetch(args)
    connection = ActiveRecord::Base.connection
    if args.respond_to? :upcase # duck type for string
        return connection.execute(args).to_a
    else
      to_a          = args.fetch(:to_a, true)
      connection    = args.fetch(:connection, ActiveRecord::Base.connection)
      query         = args.fetch(:query)
      single_result = args.fetch(:single_result, false)
      if single_result # return single result if passed as argument. Otherwise, return an array
        return connection.execute(query).first.values.first
      else
        if to_a
          return connection.execute(query).to_a
        else
          return connection.execute(query)
        end
      end
    end
  end

  #Similar to fetch, but returns complex types like array as valid ruby arrays
  def self.fetch_with_type(args)
    #Merge of self.fetch and of this stackoverflow answer : https://stackoverflow.com/a/30948357
    #This might be mergeable with fetch, but it will need testing before confirmation
    connection = ActiveRecord::Base.connection
    @type_map ||= PG::BasicTypeMapForResults.new(connection.raw_connection)
    if args.respond_to? :upcase # duck type for string
        result = connection.execute(args)
        result.type_map = @type_map
        return result.to_a
    else
      to_a          = args.fetch(:to_a, true)
      connection    = args.fetch(:connection, ActiveRecord::Base.connection)
      query         = args.fetch(:query)
      single_result = args.fetch(:single_result, false)
      if single_result # return single result if passed as argument. Otherwise, return an array
        result = connection.execute(query).first.values.first
        result.type_map = @type_map
        return result
      else
        if to_a
          result = connection.execute(query)
          result.type_map = @type_map
          return result.to_a
        else
          result = connection.execute(query)
          result.type_map = @type_map
          return result
        end
      end
    end
  end
  
  def self.fetch_symbolize_keys(args)
    result = TrDbFetcher.fetch(args)
    return result.respond_to?(:each) ? result.map{|line|line.symbolize_keys} : result
  end
  
  def self.fetch_single(query)
    TrDbFetcher.fetch(:query => query, :single_result => true)
  end
  

  def self.reset_pk_sequences(data_classes)
    #Reset primary keys for a list of classes
    data_classes.each do |data_class|
      data_class.send(:reset_pk_sequence)
    end
  end
  
end
