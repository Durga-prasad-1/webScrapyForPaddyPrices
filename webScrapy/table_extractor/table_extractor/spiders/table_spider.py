import scrapy
from table_extractor.items import TableExtractorItem
from datetime import datetime

today = datetime.today().strftime("%d-%b-%Y")
# today = "08-May-2025"

class TableSpider(scrapy.Spider):
    name = 'table_spider'

    def start_requests(self):
        url = (
            f"https://agmarknet.gov.in/SearchCmmMkt.aspx?"
            f"Tx_Commodity=2&Tx_State=TL&Tx_District=0&Tx_Market=0"
            f"&DateFrom={today}&DateTo={today}&Fr_Date={today}&To_Date={today}"
            f"&Tx_Trend=0&Tx_CommodityHead=Paddy(Dhan)(Common)"
            f"&Tx_StateHead=Telangana&Tx_DistrictHead=--Select--&Tx_MarketHead=--Select--"
        )

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://agmarknet.gov.in/',
            'Connection': 'keep-alive',
        }

        self.logger.info(f"Sending request to {url} with headers")
        yield scrapy.Request(url=url, headers=headers, callback=self.parse)

    def parse(self, response):
        if response.status == 403:
            self.logger.warning("Received 403 Forbidden - Possible bot block or missing headers")
            with open("403.html", "wb") as f:
                f.write(response.body)

        table = response.css('table.tableagmark_new')

        for row in table.css('tr')[1:]:  # Skip header row
            item = TableExtractorItem()
            row_data = []

            for td in row.css('td'):
                span_text = td.css('span::text').get()
                if span_text:
                    row_data.append(span_text.strip())
                else:
                    row_data.append(' '.join(td.css('::text').getall()).strip())

            if row_data:
                item['sl_no'] = row_data[0]
                item['district'] = row_data[1]
                item['market'] = row_data[2]
                item['commodity'] = row_data[3]
                item['variety'] = row_data[4]
                item['grade'] = row_data[5]
                item['min_price'] = row_data[6]
                item['max_price'] = row_data[7]
                item['modal_price'] = row_data[8]
                item['price_date'] = row_data[9]

                yield item
