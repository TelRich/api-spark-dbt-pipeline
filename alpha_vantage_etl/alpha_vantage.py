#%%
import requests as re
import json
import yaml
from sqlalchemy import create_engine

config = yaml.safe_load(open("config.yaml", "r"))

class AlphaVantageWrapper:
    def __init__(self, config):
        self.api_key = config['API_KEY']
        self.base2_url = "https://www.alphavantage.co/query?function={}&apikey={}"
        self.base3_url = "https://www.alphavantage.co/query?function={}&{}&apikey={}"
        
        # Create a MySQL engine
        self.mysql_engine = create_engine(
            f"mysql://{config['USER']}:{config['PASSWORD']}@{config['HOST']}:{config['PORT']}/{config['DB_NAME']}"
        )
        
    def make_base2_request(self, function_name):
        url = self.base2_url.format(function_name, self.api_key)
        r = re.get(url)
        return r.status_code, r.json()
    
    def make_base3_request(self, function_name, parameter):
        url = self.base3_url.format(function_name, parameter, self.api_key)
        r = re.get(url)
        return r.status_code, r.json()
    
    def search_symbol(self, function_name, parameter):
        return self.make_base3_request(function_name, parameter)
    
    def save_to_mysql(self, df, table_name):
        # Save a DataFrame to the MySQL database
        df.to_sql(table_name, con=self.mysql_engine, if_exists='append', index=False)

if __name__ == "__main__":
    alpha_vantage = AlphaVantageWrapper(config)
    print(alpha_vantage.search_symbol("SYMBOL_SEARCH", "keywords=IBM")[0])
#%% 