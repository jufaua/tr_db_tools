module TrDbTools
  
  def self.included(base)
    base.extend(ClassMethods)
  end
  
  module ClassMethods
    def reset_pk_sequence
      TrDbFetcher.execute("SELECT setval('#{self.table_name}_id_seq',(SELECT COALESCE(MAX(id),0)+1 FROM #{self.table_name}), false);")
    end
    
    def get_next_pk
      TrDbFetcher.fetch_single("SELECT (COALESCE(MAX(#{self.primary_key.to_s}),0)+1) as result FROM #{self.table_name}").to_i
    end
  end
  
end
require 'tr_db_tools/tr_db_fetcher'