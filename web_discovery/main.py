import json
import web_discovery_searx as searx
if __name__ == "__main__":

    f = open('./config.json', 'r')
    config = json.loads(f.read())
    f.close()

    sites = searx.web_discovery_with_searx(config['web_discovery']['id_seed_path'],
                                                         config['web_discovery']['searx']['num_of_searx_result_pages'],
                                                         config['web_discovery']['searx']['save_web_discovery_output'],
                                                         config['web_discovery']['searx'][
                                                             'path_to_save_web_discovery_output'])

