import searx


'''Input path to id file, number of pages of the search output for searx'''
def web_discovery_with_searx(seed, number_result_pages, searx_address):

    seeds = open(seed,'r')
    results = []
    for seed in seeds:
        for pageno in range (number_result_pages):
            output = searx.searx_request(id = seed,pageno = pageno,searx_address = searx_address)
            list = output['results']
            for result in list:
                results.append(result['url'])
    return results

