#%%
import requests as re
import json
import yaml

config = yaml.safe_load(open("config.yaml", "r"))

class AlphaVantageWrapper:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base2_url = "https://www.alphavantage.co/query?function={}&apikey={}"
        self.base3_url = "https://www.alphavantage.co/query?function={}&{}&apikey={}"
        
    def make_base2_request(self, function_name):
        url = self.base2_url.format(function_name, self.api_key)
        r = re.get(url)
        return r.status_code, r.json()
    
    def make_base3_request(self, function_name, other_parameter):
        url = self.base3_url.format(function_name, other_parameter, self.api_key)
        r = re.get(url)
        return r.status_code, r.json()
    
    def search_symbol(self, function_name, parameter):
        return self.make_base3_request(function_name, parameter)

if __name__ == "__main__":
    alpha_vantage = AlphaVantageWrapper(api_key=config['API_KEY'])
    print(alpha_vantage.search_symbol("OVERVIEW", "symbol=IBM")[0])
#%% 