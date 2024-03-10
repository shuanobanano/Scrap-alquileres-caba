import pandas as pd
import numpy as np
from botasaurus import *
import time
import re
import requests
from typing import Literal
from datetime import datetime
from src.constants import zona_prop_url, max_number_pages_zonaprop

def _get_page_number_url(number:int, type_building:str, type_operation:str) -> str:
    url = zona_prop_url + type_building + f"-{type_operation}-capital-federal-pagina-{number}.html"
    return url

def _get_url_list(max_number:int, type_building:str, type_operation:str) -> list[str]:
    request = AntiDetectRequests()
    response = request.get(_get_page_number_url(max_number, type_building, type_operation), allow_redirects=True)
    last_page_url = response.url
    match = re.search(r'(\d+)\.html$', last_page_url)
    if match:
        last_page_number = (match.group(1))
        page_list = [_get_page_number_url(i, type_building, type_operation) for i in range(1, int(last_page_number) + 1)]
        return page_list
    else:
        print("Could not find last webpage, try again in a few minutes")

def _parse_property_listings(soup, posting_container_class:str) -> list:
    """Parses property listings from a BeautifulSoup object.

    Args:
        soup (BeautifulSoup): A BeautifulSoup object containing the HTML content.

    Returns:
        list: A list of dictionaries, each representing a property listing.
    """
    property_elements = soup.find_all(class_ = posting_container_class) #this should be a list with each posting_container class element
    properties = []
    # print("Propiedad dentro de _parse", property_elements)
    for property_element in property_elements:
        try:
            properties.append(_parse_property(property_element))
            # print("Se appendio la propiedad", properties)
            if len(properties == 0):
                print("be aware of the div selected in the soup, it usually changes.") #this should already be solved
                break
        except Exception:
            # print("No se appendio ninguna propiedad")
            #There are 'Developing' buildings with a range of prices. 
            pass
    # print("Propiedades final de _parse", properties)
    return properties

def _parse_property(property_element) -> dict:
    """Parses an individual property from the property element.

    Args:
        property_element (Tag): A BeautifulSoup Tag representing a property element.

    Returns:
        dict: A dictionary containing property details.
    """
    id_element = property_element.find(attrs={"data-id": True})['data-id']
    price_element = property_element.find(attrs={"data-qa":"POSTING_CARD_PRICE"}).text
    location_element = property_element.find(attrs={'data-qa': 'POSTING_CARD_LOCATION'}).text
    address_element = property_element.find(class_='postingAddress').text
    features_elements = [span.text for span in property_element.find(attrs={'data-qa': 'POSTING_CARD_FEATURES'}).find_all('span')]
    description_element = property_element.find(attrs={'data-qa': 'POSTING_CARD_DESCRIPTION'}).text
    expensas_element = property_element.find(attrs={'data-qa': 'expensas'}).text
    ap_link_element = property_element.find(attrs={"data-to-posting": True})['data-to-posting']
    # print("price_element", price_element)
    return {
        'id': id_element if id_element else np.nan,
        'Price': price_element if price_element else np.nan,
        'Location': location_element if location_element else np.nan,
        'Address': address_element if address_element else np.nan,
        # 'Has_photo': has_photo,
        'Features': [feature_element for feature_element in features_elements],
        # 'Summarize': summarize_element if summarize_element else np.nan,
        'Description': description_element if description_element else np.nan,
        'Expensas': expensas_element if expensas_element else np.nan,
        'Link': zona_prop_url[:-1] + ap_link_element if ap_link_element else np.nan,
    }

def _get_posting_container_class(soup):
    posting_container = soup.find(class_='postings-container')
    classes_inside_posting_container = []
    for child in posting_container.children:
        if child.name and child.get('class'):
            classes_inside_posting_container.extend(child['class'])
    return classes_inside_posting_container[0]
    
def _scrape_property_listings(request: AntiDetectRequests, 
                              url_list: list[str],
                              ) -> list:
    """Scrapes property listings from ZonaProp.

    Args:
        request (AntiDetectRequests): An instance of AntiDetectRequests.
        link (str): The URL of the property listings page.

    Returns:
        list: A list of dictionaries, each representing a property listing.
    """
    properties = []
    # itereation_count = 0#!debug
    # while True:
    for link in url_list:
        # itereation_count += 1#!debug
        print(link)
        try:
            soup = request.bs4(link)
            posting_container_class = _get_posting_container_class(soup)
            # print("sopita", soup)
            properties += _parse_property_listings(soup, posting_container_class)
            print("propiedades dentro del try",_parse_property_listings(soup, posting_container_class))
            # # Find the next page button
            # next_page = soup.select_one('a[data-qa="PAGING_NEXT"]')
            # if itereation_count >= 1:#!debug
                # break#!debug
            # if next_page and next_page['href']:
            #     # Update the link for the next iteration
            #     link = zona_prop_url + next_page['href']
            #     print(next_page['href'])
            # else:
            #     # No more pages, break the loop
            #     break
        except requests.exceptions.HTTPError as e:
            print(f"HTTPError occurred: {e}. Retrying in 15 minutes.")
            time.sleep(15*60) # Sleep for 15 minutes
        except Exception as e:
            print(f"An unknown error occurred: {e}.")
            break
    return properties

def _export_scrap_zonaprop(scrap_results:dict,
                           type_operation:str,
                           type_building:str):
    date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    pd.DataFrame(scrap_results).to_parquet(f"./output/zonaprop_{type_operation}_{type_building}_{date_str}.pkt")

def main_scrap_zonaprop(
    type_operation: Literal["alquiler", "venta"] = "alquiler", #bug with "venta"
    type_building:Literal["locales-comerciales", "departamentos","oficinas-comerciales"] = "departamentos",
    export_final_results:bool = True,
                        ) -> list | None:
    """Runs the main process of scraping property listings from ZonaProp.

    Returns:
        list: A list of dictionaries, each representing a property listing.
    """
    url_list =  _get_url_list(max_number_pages_zonaprop, type_building, type_operation)
    print("Max html page:", url_list[-1])
    try:
        request = AntiDetectRequests()
        # url = zona_prop_url + type_building + f"-{type_operation}-capital-federal.html"
        final_dict = _scrape_property_listings(request, url_list)
        print(final_dict)
        if export_final_results:
            _export_scrap_zonaprop(final_dict, type_operation ,type_building)
            print("Results exported correctly")
        return final_dict
    except Exception as e:
        print(f"An error occurred: {e}")