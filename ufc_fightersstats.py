#using scrapy
import scrapy

# # Install Scrapy first (only needed once per environment)
# import scrapy
# from scrapy.crawler import CrawlerProcess

# class UniversalSpider(scrapy.Spider):
#     name = "universal_spider"

#     def __init__(self, start_url=None, selector=None, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.start_urls = [start_url] if start_url else []
#         self.selector = selector

#     def parse(self, response):
#         # Use the selector passed when starting crawl
#         if self.selector:
#             data = response.css(self.selector).getall()
#             for item in data:
#                 yield {'data': item}



# process = CrawlerProcess()

# # Crawl 1: UFC stats example
# process.crawl(
#     UniversalSpider,
#     start_url="http://ufcstats.com/statistics/fighters?char=a&page=all",
#     selector="td.b-statistics__table-col a::text"
# )

# # Crawl 2: Example site, scraping headlines (replace with real URL & selector)
# process.crawl(
#     UniversalSpider,
#     start_url="https://news.ycombinator.com/",
#     selector="a.storylink::text"
# )

# # Add more crawls here as needed...

# process.start()  # Start crawling all


class UFCFightSpider(scrapy.Spider):
    name = "ufc_fighters"

    number_of_fighters = 20  # limit number of events to scrape

    # fight details fields
    fighter_fields = [
        'firstname','lastname','nickname','nickname'
        'fighter_A_KD', 'fighter_B_KD',
        'fighter_A_SIG_STR', 'fighter_B_SIG_STR',
        'fighter_A_SIG_STR%', 'fighter_B_SIG_STR%',
        'fighter_A_TOTAL_STR', 'fighter_B_TOTAL_STR',
        'fighter_A_TD', 'fighter_B_TD',
        'fighter_A_TD%', 'fighter_B_TD%',
        'fighter_A_SUB_ATT', 'fighter_B_SUB_ATT',
        'fighter_A_REV', 'fighter_B_REV',
        'fighter_A_CTRL', 'fighter_B_CTRL',
        'Winner'
    ]

    def __init__(self, *args, **kwargs):
        super(UFCFightSpider, self).__init__(*args, **kwargs)
        self.fight_data = {key: [] for key in self.fight_fields}

    def check_html(self, link):
        return link.endswith('.html')

    def parse(self, response):
        # Select x event links
        # events_completed = response.css('tr.b-statistics__table-row td i a::attr(href)').getall()[:self.number_of_events]

    
        # # Select event links
        events_completed = set(response.css('tr.b-statistics__table-row td i a::attr(href)').getall())

        self.logger.info(f"Found {len(events_completed)} events, scraping each.")

        #for each event link check if its a real link then proceed to parse_event to extract details of all fights
        for event_url in events_completed:
            # print(f"Processing event URL: {event_url}")
            if self.check_html(event_url+'.html') and 'http://ufcstats.com/event-details/' in event_url:
                yield response.follow(event_url, callback=self.parse_event)

    def parse_event(self, response):
        # Collect unique fight detail links from event page
        fight_links = set(response.css('tr[data-link]::attr(data-link)').getall())

        self.logger.info(f"Found {len(fight_links)} fights in event {response.url}")

        #for each link if its a legit fight link then proceed to parse_fight to extract fight details
        for fight_url in fight_links:
            # print(f"Processing fight URL: {fight_url}")
            if self.check_html(fight_url+'.html') and "fight-details" in fight_url:
                yield response.follow(fight_url, callback=self.parse_fight)

    def parse_fight(self, response):
        # Extract winner
        winner = None
        winner_i = response.css('div.b-fight-details__person i.b-fight-details__person-status.b-fight-details__person-status_style_green')
        if winner_i:
            winner_container = winner_i.xpath('ancestor::div[contains(@class,"b-fight-details__person")]')[0]
            winner_tag = winner_container.css('div h3 a::text').get()
            winner = winner_tag.strip() if winner_tag else None
        else:
            # Check for NC or D results
            gray_status = response.css('div.b-fight-details__person i.b-fight-details__person-status.b-fight-details__person-status_style_gray::text').get()
            if gray_status in ['NC', 'D']:
                winner = gray_status
            else:
                winner = "Unknown"

        self.fight_data['Winner'].append(winner)

        # Extract fight stats table - the second 'tr' element (index 1)
        fight_stats_tr = response.css('tr')[1]
        if not fight_stats_tr:
            self.logger.warning(f"No fight stats table found for fight {response.url}")
            # Append Nones for all fields except winner to keep lengths consistent
            for key in self.fight_fields:
                if key != 'Winner':
                    self.fight_data[key].append(None)
            return

        tds = fight_stats_tr.css('td')

        try:
            self.fight_data['fighter_A'].append(tds[0].css('p a::text').get(default='').strip())
            self.fight_data['fighter_B'].append(tds[0].css('p')[1].css('a::text').get(default='').strip())

            self.fight_data['fighter_A_KD'].append(tds[1].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_KD'].append(tds[1].css('p')[1].css('::text').get(default='').strip())

            self.fight_data['fighter_A_SIG_STR'].append(tds[2].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_SIG_STR'].append(tds[2].css('p')[1].css('::text').get(default='').strip())

            self.fight_data['fighter_A_SIG_STR%'].append(tds[3].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_SIG_STR%'].append(tds[3].css('p')[1].css('::text').get(default='').strip())

            self.fight_data['fighter_A_TOTAL_STR'].append(tds[4].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_TOTAL_STR'].append(tds[4].css('p')[1].css('::text').get(default='').strip())

            self.fight_data['fighter_A_TD'].append(tds[5].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_TD'].append(tds[5].css('p')[1].css('::text').get(default='').strip())

            self.fight_data['fighter_A_TD%'].append(tds[6].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_TD%'].append(tds[6].css('p')[1].css('::text').get(default='').strip())

            self.fight_data['fighter_A_SUB_ATT'].append(tds[7].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_SUB_ATT'].append(tds[7].css('p')[1].css('::text').get(default='').strip())

            self.fight_data['fighter_A_REV'].append(tds[8].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_REV'].append(tds[8].css('p')[1].css('::text').get(default='').strip())

            self.fight_data['fighter_A_CTRL'].append(tds[9].css('p::text').get(default='').strip())
            self.fight_data['fighter_B_CTRL'].append(tds[9].css('p')[1].css('::text').get(default='').strip())

        except Exception as e:
            self.logger.error(f"Error parsing fight stats for {response.url}: {e}")
            # Append None to all fields except Winner to maintain consistent list lengths
            for key in self.fight_fields:
                if key != 'Winner' and len(self.fight_data[key]) < len(self.fight_data['Winner']):
                    self.fight_data[key].append(None)

    def closed(self, reason):
        # Called when spider finishes scraping - output results to csv
        import pandas as pd
        for column in self.fight_data:
            print(column)
            print('length '+str(len(self.fight_data[column])))
        
        number_of_fights=8279 
        df = pd.DataFrame({k: v[:number_of_fights] for k, v in self.fight_data.items()})
        # df = pd.DataFrame(self.fight_data)
        self.logger.info(f"Scraped {len(df)} fights.")
        # Save dataframe or do whatever you want
        df.to_excel("ufc_fights.xlsx", index=False)
        self.logger.info("Saved fights data to ufc_fights.xlsx")

from scrapy.crawler import CrawlerProcess

import time
start_time = time.time()

process = CrawlerProcess(settings={
    "LOG_LEVEL": "DEBUG",  # or "DEBUG" for even more detail
})
process.crawl(UFCFightSpider)
process.start()
print("Scraping completed.")
print(f"Total execution time: {time.time() - start_time:.2f} seconds")