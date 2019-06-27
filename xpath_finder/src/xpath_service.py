import ast, itertools, re
import text_parser, mongodb_interface
from lxml import etree,html
from collections import defaultdict

def find_xpath(domain, list_pages):
    map_xpath = defaultdict(list)
    domain_xpath_map = defaultdict(set)
    xpaths_obj_list = []
    featured_page_list = []
    list_temp_2 = []
    list_temp_flattened = []
    for page_dict in list_pages:
        struct = (page_dict['url'], page_dict['cluster_label'], page_dict['referring_url'])
        xpaths = url_2_xpath(domain, struct, [])
        if (xpaths is not None) & (xpaths is not []):
            obj = (struct[0], xpaths, struct[1], struct[2])
            featured_page_list.append(obj)
    for (url,xpath_list,label,referring_url) in featured_page_list:
        for xpath_ in xpath_list:
            key = str(xpath_) + '/----/' + str(domain)
            domain_xpath_map[key].add((url, label, referring_url))
    for (key, set_) in domain_xpath_map.items():
        list_ = []
        for elem in set_:
            list_.append(elem)
        domain_xpath_map[key] = list_
    for (key, values) in domain_xpath_map.items():
        obj = (key.split('/----/')[0],(key.split('/----/')[1],values))
        list_temp_2.append(obj)
    list_temp = []
    for (xpath,(domain,urls)) in list_temp_2:
        for url in urls:
            obj = (xpath,(domain,url))
            list_temp_flattened.append(obj)
    for (xpath,(domain,url)) in list_temp_flattened:
        key = str(domain)+'/----/'+str(url[1])
        obj = (ast.literal_eval(xpath),url[0],url[2])
        map_xpath[key].append(obj)

    for (key,values) in map_xpath.items():
        obj = ((key.split('/----/')[0],key.split('/----/')[1]),list(values))
        xpaths_obj_list.append(obj)
    return xpaths_obj_list

# Function to calculate the sequence of xpaths from home page to the given url
def url_2_xpath(domain,cluster_element,xpath_list,number_of_recursions=9):
    if (cluster_element[2][0] == 'b'):
        referring_url = cluster_element[2][2:-1]
    else:
        referring_url = cluster_element[2]
    if (number_of_recursions==1) or ((domain == cluster_element[0]) and (domain == referring_url)) or \
        (('http://' + text_parser.remove_www_domain(domain) == cluster_element[0]) and ('http://' + text_parser.remove_www_domain(domain) == referring_url))or \
        (('https://' + text_parser.remove_www_domain(domain) == cluster_element[0]) and ('https://' + text_parser.remove_www_domain(domain) == referring_url)):
        result = []
        for element in itertools.product(*xpath_list):
            l = list(element)
            l.reverse()
            result.append(l)
        return result
    try:
        html_text = mongodb_interface.get_html_page(domain, referring_url)
        root = html.fromstring(html_text)
        current_xpath_list = []
    except:
        return None
    tree = etree.ElementTree(root)
    for e in root.iter():
        if e.get('href') == str(cluster_element[0]) or \
            e.get('href') == str('/'+re.sub('.*://.*?/','',str(cluster_element[0]))):
            current_xpath_list.append(tree.getpath(e))

    xpath_list.append(current_xpath_list)
    parent_cluster_element=(referring_url,
                            mongodb_interface.get_depth_level(domain,referring_url),
                            mongodb_interface.get_referring_url(domain,referring_url))
    final_result_list = url_2_xpath(domain,parent_cluster_element,xpath_list,number_of_recursions-1)
    return final_result_list


def generalize_xpath(xpath_list):
    map_xpath = defaultdict(list)
    xpath_list_filtered = []
    xpath_list_flattened = []
    xpath_sliced_list = []
    result = []
    for (key, values) in xpath_list:
        if (values is not None) and (values is not []):
            xpath_list_filtered.append((key, values))
    for ((domain, label), values) in xpath_list_filtered:
        for value in values:
            if value != None and value != [] and len(value) > 1 and value != {}:
                obj = ((domain, label), value)
                xpath_list_flattened.append(obj)
    xpath_list_filtered = []
    for ((domain, label), value) in xpath_list_flattened:
        obj = slice_xpath_sequence_to_last_step(domain, label, value)
        value_ = obj[1]
        if (value_ is not None) and (value_ is not []) and (value_ is not '') and (value_ is not {}):
            xpath_sliced_list.append(obj)
    for ((domain, label), (xpath_list, last_xpath, url, referring_url)) in xpath_sliced_list:
        key = str(domain) + '/----/' + str(label) + '/----/' + str(xpath_list)
        obj = (last_xpath, url, referring_url)
        map_xpath[key].append(obj)
    for (key, value) in map_xpath.items():
        obj = ((key.split('/----/')[0], key.split('/----/')[1], key.split('/----/')[2]), list(value))
        generalized_xpath = xpath_index_generalization(obj[1])
        if len(generalized_xpath[0]) > 0:
            struct = (obj[0], generalized_xpath)
            xpath_list_filtered.append(struct)
    for ((domain, label, xpath), (generalized_xpath, xpaths, url2ref)) in xpath_list_filtered:
        obj = ((domain, label, xpath), (generalized_xpath, len(xpaths), xpaths, url2ref))
        result.append(obj)
    return result

# Generalize a specific index in the xpath expression
def xpath_index_generalization(xpath_list):
    xpaths = []
    url2ref = []
    generalized_xpath = []
    for xpath in xpath_list:
        xpaths.append(xpath[0])
        url2ref.append((xpath[1], xpath[2]))
    for xpath in xpaths:
        for xpath2 in xpaths:
            if xpath != xpath2 and len(xpath) == len(xpath2):
                xpath_tags_1 = xpath.split('/')[1:]
                xpath_tags_2 = xpath2.split('/')[1:]
                n = len(xpath_tags_1)
                change_indexes = []
                for index in range(0, n):
                    try:
                        if xpath_tags_1[index] != xpath_tags_2[index]:
                            change_indexes.append(index)
                    except:
                        continue
                for index in change_indexes:
                    xpath_tags_1[index] = re.sub('\[.*\]', '[*]', xpath_tags_1[index])
                    generalized_xpath.append('/' + '/'.join(xpath_tags_1))

    generalized_xpath = list(set(generalized_xpath))
    generalized_xpath = list(set(xpath_max_generalization(generalized_xpath)))
    return (generalized_xpath, xpaths, url2ref)

# Generalize the entire xpath expression
def xpath_max_generalization(generalized_xpath):
    path_dict = {}
    output = []

    for path in generalized_xpath:
        current_path = re.sub('\[[0-9]*\]|\[[\*]*\]', '', path)
        if current_path in path_dict:
            path_dict[current_path].append(path)
        else:
            path_dict[current_path] = [path]
    for path in path_dict:
        max_count = max(path_dict[path], key=lambda x: x.count('*'))
        output.append(max_count)
    return output


# It returns the xpaths in this form ([xpath1,xpath2,...,xpathN-1],xpathN,url,referring_url)
def slice_xpath_sequence_to_last_step(domain, label, value):
    try:
        output = (value[0][:len(value[0]) - 1], value[0][-1], value[1], value[2])
        return ((domain,label),output)
    except:
        return ((domain,label),None)
