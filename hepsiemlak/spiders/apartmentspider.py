import time
import scrapy


class ApartmentSpider(scrapy.Spider):
    name = "apartmentspider"
    allowed_domains = ["hepsiemlak.com"]
    start_urls = ["https://www.hepsiemlak.com/istanbul-satilik/daire"]

    crawl_count = 0

    def parse(self, response):
        # get the section with all listings
        listings = response.css('div.links')

        # get the link to each listing and send the spider to the listing page
        for advert in listings:
            next_url_part = advert.css('a::attr(href)').get()

            if next_url_part is not None:
                next_url_full = "https://www.hepsiemlak.com" + next_url_part
                yield response.follow(next_url_full, callback=self.get_apart_info)

        # after every link in the page is crawled, get the url for next page
        next_page = response.css('a.he-pagination__navigate-text.he-pagination__navigate-text--next ::attr(href)').get()

        # if there is a next page call this function again and do it all over again
        if next_page is not None:
            next_page_url = 'https://www.hepsiemlak.com' + next_page
            yield response.follow(next_page_url, callback=self.parse)

    def get_apart_info(self, response):
        # create a dictionary to store retrieved information
        info = {
            "Başlık": None,
            "Fiyat": None,
            "Oda + Salon Sayısı": None,
            "Brüt / Net M2": None,
            "Bulunduğu Kat": None,
            "Bina Yaşı": None,
            "Isınma Tipi": None,
            "Kat Sayısı": None,
            "Eşya Durumu": None,
            "Banyo Sayısı": None,
            "Kullanım Durumu": None,
            "Tapu Durumu": None,
            "İlçe": None,
            "Mahalle": None,
            "Link": response.url
        }

        # get all needed information in the table according to the keys in the dictionary
        for li in response.css('li.spec-item'):
            first_span_text = li.css('span.txt::text').get()
            if first_span_text in info:
                value = li.css('span::text').getall()[1].strip()
                info[first_span_text] = value

        # get place information from above the table and put them in the dict
        place_info = response.css('ul.short-property li::text').getall()
        district = place_info[1].strip()
        neighborhood = place_info[2].strip()
        info["İlçe"] = district
        info["Mahalle"] = neighborhood

        # get title and price information and put them in the dict
        price = response.css('p.fz24-text.price::text').get().strip()
        title = response.css('h1.fontRB::text').get().strip()
        info["Fiyat"] = price
        info["Başlık"] = title

        yield info

        # slow down crawling to avoid 429 error
        self.crawl_count += 1

        if self.crawl_count % 40 == 0:
            self.log(f"Scraped {self.crawl_count} times. Pausing for 120 seconds...")
            time.sleep(120)
