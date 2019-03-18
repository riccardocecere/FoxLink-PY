import json
import web_discovery_searx as searx

def main():
    f = open('./config.json', 'r')
    config = json.loads(f.read())
    f.close()

    sites = searx.web_discovery_with_searx(config['web_discovery']['id_seed_path'],
                                           config['web_discovery']['searx']['num_of_searx_result_pages'],)

    sites_set = set()
    for elem in sites:
        sites_set.add(elem)

if __name__ == "__main__":

    main()