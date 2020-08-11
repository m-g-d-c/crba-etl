
# functions extracting basic information about an sdmx structure from a sdmx-json file


# get number of dimensions from sdmx_json_struc file
def get_sdmx_dim(sdmx_json_struc):
    # number of dimensions in SDMX dataflow
    n_dim = len(sdmx_json_struc['structure']['dimensions']['observation'])
    return n_dim


# Parse sdmx_json_struc file to get countries codes in sdmx dataflow
# Input: country names list, sdmx JSON file with dataflow structure
# Returns: dictionary, (keys) country names,(values) country codes for dataflow API requests

from difflib import get_close_matches

def get_country_code(country_list,sdmx_json_struc):

    country_code = {}
    country_discard = {}

    for elem in sdmx_json_struc['structure']['dimensions']['observation'][0]['values']:
        if elem['name'].lower() in country_list:
            country_code[elem['name'].lower()] = elem['id']
        else:
            country_discard[elem['name'].lower()] = elem['id']

    # check countries not found
    not_found = [k for k in country_list if k not in country_code]
    # uses previous knowledge to avoid comparing string 'republic' (function can be generalized)
    for elem in not_found:
        matched_country = get_close_matches(elem[:5], country_discard,1)[0]
        # add the matched_country in dictionary (note: key retained from country_list)
        country_code[elem] = country_discard[matched_country]

    # return dictionary ordered with country list
    ordered_dict = {k:country_code[k] for k in country_list}
    return ordered_dict
