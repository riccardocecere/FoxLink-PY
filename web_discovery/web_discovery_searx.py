#from web_discovery import searx
#from general_utils import rdd_utils, text_parser
import types

import searx


'''Input path to id file, number of pages of the search output for searx'''
def web_discovery_with_searx(path,num_of_pages):

    seeds = open(path,'r')
    results = []
    for seed in seeds:
        for pageno in range (num_of_pages):
            output = searx.searx_request(seed,pageno)
            list = output['results']
            for result in list:
                results.append(result['url'])

        print('#####################')
        print(results)
        print(type(results))
    return results

