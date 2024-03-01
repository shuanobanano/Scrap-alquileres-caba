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
    else:
        print("Could not find last webpage, try again in a few minutes")
    page_list = [_get_page_number_url(i, type_building, type_operation) for i in range(1, int(last_page_number) + 1)]
    return page_list

def _parse_property_listings(soup) -> list:
    """Parses property listings from a BeautifulSoup object.

    Args:
        soup (BeautifulSoup): A BeautifulSoup object containing the HTML content.

    Returns:
        list: A list of dictionaries, each representing a property listing.
    """
    property_elements = soup.select('div.sc-1tt2vbg-5.GcsXo')
    properties = []
    # print("Propiedad dentro de _parse", property_elements)
    for property_element in property_elements:
        try:
            properties.append(_parse_property(property_element))
            # print("Se appendio la propiedad", properties)
            if len(properties == 0):
                print("be aware of the div selected in the soup, it usually changes.")
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
    price_element = property_element.select_one('div[data-qa="POSTING_CARD_PRICE"]')
    location_element = property_element.select_one('div[data-qa="POSTING_CARD_LOCATION"]')
    address_element = property_element.select_one('div[class="sc-ge2uzh-0 eXwAuU"]') 
    photo_elements = property_element.select('img')
    has_photo = any(photo.get('src').endswith('isFirstImage=true') for photo in photo_elements) #Does not work because of "lazy" js
    features_elements = property_element.select('div[data-qa="POSTING_CARD_FEATURES"] span')
    summarize_element = property_element.select_one('a.sc-i1odl-12.EWzaP')
    description_element = property_element.select_one('div[data-qa="POSTING_CARD_DESCRIPTION"]')
    expensas_element = property_element.select_one('div[data-qa="expensas"]')
    ap_link_element = property_element.select_one('div[data-qa="posting PROPERTY"]')['data-to-posting']
    # print("price_element", price_element)
    return {
        'Price': price_element.text if price_element else np.nan,
        'Location': location_element.text if location_element else np.nan,
        'Address': address_element.text if address_element else np.nan,
        'Has_photo': has_photo,
        'Features': [feature_element.text for feature_element in features_elements],
        'Summarize': summarize_element.text if summarize_element else np.nan,
        'Description': description_element.text if description_element else np.nan,
        'Expensas': expensas_element.text if expensas_element else np.nan,
        'Link': zona_prop_url + ap_link_element if ap_link_element else np.nan,
    }
    
    
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
            # print("sopita", soup)
            properties += _parse_property_listings(soup)
            print("propiedades dentro del try",_parse_property_listings(soup))
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