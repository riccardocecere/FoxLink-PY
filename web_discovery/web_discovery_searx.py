#from web_discovery import searx
#from general_utils import rdd_utils, text_parser
import searx


'''Input path to id file, number of pages of the search output for searx'''
def web_discovery_with_searx(path,num_of_pages):

    seeds = open(path,'r')
    results = []
    for seed in seeds:
        for pageno in range (num_of_pages):
            output = searx.searx_request(seed,pageno)
            list = output['results']
            list_urls = []
            for result in list:
                try:
                    list_urls = list_urls.append(result['url'])
                except:
                    ''
            try:
                results = results.append(list_urls)
            except:
                ''
        print('#####################')
    return results

