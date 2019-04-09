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
    return results

