import requests
from bs4 import BeautifulSoup

def scrape_prices():
    url = 'https://www.amazon.com/-/es/deal/701fcba8/?_encoding=UTF8&_encoding=UTF8&showVariations=true&moreDeals=f02b3f41%2C72d196b0&ref_=dlx_gate_dd_dcl_tlt_701fcba8_dt_pd_hp_d_atf_unk&pd_rd_w=7XQkX&content-id=amzn1.sym.488b2a1a-d1f8-4aa5-ae8e-c6a2d8bd006a&pf_rd_p=488b2a1a-d1f8-4aa5-ae8e-c6a2d8bd006a&pf_rd_r=DZ769FAT8J4GFV5ZVQHE&pd_rd_wg=PCJr2&pd_rd_r=6d3af1f4-391d-422e-b66d-db7448ad2c26/'  # Example URL
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    prices = []

    # Extract prices from the page
    for product in soup.find_all(class_='product'):
        price_tag = product.find(class_='price')
        if price_tag:
            price = price_tag.text.strip()
            prices.append(price)

    return prices

if __name__ == "__main__":
    prices = scrape_prices()
    for price in prices:
        print(price)
