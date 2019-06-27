
def check_urls(list):
    for elem in list:
        obj = elem['referring_url']
        if obj[0] is 'b':
            temp = obj[2:-1]
            elem['referring_url'] = temp
    return list
