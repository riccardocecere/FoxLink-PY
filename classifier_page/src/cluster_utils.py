import text_parser
import mongodb_interface as mongo
import os

DATABASE_READ = os.environ.get('DATABASE_READ')

'''Funzione di parsing dei cluster, portando la granularità da cluster a livello di pagina:
        input:  lista di uno o più cluster a dizionario
        output: lista di elementi (dominio, (url_pagina, url_pagina_referente, etichetta, testo html))
'''
def parse_cluster(domain, clusters):
    cluster_list = []
    for cluster in clusters:
        cluster_list.append((cluster['cluster_elements'],cluster['label']))

    result_list = []
    for (cluster_elements, label) in cluster_list:
        for element in cluster_elements:
            url = element[0]
            referring_url = element[2]
            html = mongo.get_html_page(DATABASE_READ,domain, url)
            page_text = text_parser.get_clean_text_from_html(html)
            page_dict = {
                'category': 1,
                'text': page_text,
                'url': url,
                'referring_url': referring_url,
                'cluster_label': label
            }
            result_list.append(page_dict)

    return result_list
