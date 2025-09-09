
#using scrapy
import scrapy
import json
import os
from datetime import datetime

SCRAPED_FIGHTS_FILE = 'scraped_fights.json'
SCRAPED_EVENTS_FILE = 'scraped_events.json'

UNSCRAPED_FIGHTS_FILE = 'unscraped_fights.json'  # a file you prepare with failed URLs

# Keep track of previously scraped fights and events
if os.path.exists(SCRAPED_FIGHTS_FILE):
    with open(SCRAPED_FIGHTS_FILE, 'r') as f:
        scraped_fights_set = set(json.load(f))
else:
    scraped_fights_set = set()



if os.path.exists(SCRAPED_EVENTS_FILE):
    try:
        with open(SCRAPED_EVENTS_FILE, 'r') as f:
            scraped_events_set = set(json.load(f))
    except json.JSONDecodeError:
        # File is empty or invalid, start with empty set
        scraped_events_set = set()
else:
    scraped_events_set = set()


# Load unscraped/fail fights
if os.path.exists(UNSCRAPED_FIGHTS_FILE):
    try:
        with open(UNSCRAPED_FIGHTS_FILE, 'r') as f:
            all_failed_fights = set(json.load(f))

    except json.JSONDecodeError:
        all_failed_fights = set()
else:
    all_failed_fights = set()


class UFCFightSpider(scrapy.Spider):
    name = "ufc_fights"
    # allowed_domains = ["ufcstats.com", "https://www.ufc.com/athletes/all"]
    start_urls = ["http://ufcstats.com/statistics/events/completed?page=all"]
    # start_urls=[]

    number_of_events = 20  # limit number of events to scrape

    fight_fields = [
        'fighter_A','fighter_B',
        'fighter_A_KD', 'fighter_B_KD',
        'fighter_A_SIG_STR', 'fighter_B_SIG_STR',
        'fighter_A_SIG_STR%', 'fighter_B_SIG_STR%',
        'fighter_A_TOTAL_STR', 'fighter_B_TOTAL_STR',
        'fighter_A_TD', 'fighter_B_TD',
        'fighter_A_TD%', 'fighter_B_TD%',
        'fighter_A_SUB_ATT', 'fighter_B_SUB_ATT',
        'fighter_A_REV', 'fighter_B_REV',
        'fighter_A_CTRL', 'fighter_B_CTRL',
        'Winner',
        'linktofight'
    ]

    # # Reduce concurrency and enable throttling
    # custom_settings = {
    # # Increase parallel requests
    # "CONCURRENT_REQUESTS": 16,       # default Scrapy is 16, safe for most sites
    # "CONCURRENT_REQUESTS_PER_DOMAIN": 8,  # limit per domain
    # "CONCURRENT_REQUESTS_PER_IP": 8,

    # # Reduce delay between requests
    # "DOWNLOAD_DELAY": 0,              # 0 seconds, fastest
    # "RANDOMIZE_DOWNLOAD_DELAY": True, # adds a little randomness to avoid blocking

    # # Enable auto-throttle to avoid getting blocked if server slows down
    # "AUTOTHROTTLE_ENABLED": True,
    # "AUTOTHROTTLE_START_DELAY": 0.5,
    # "AUTOTHROTTLE_MAX_DELAY": 3,
    # "AUTOTHROTTLE_TARGET_CONCURRENCY": 8.0, 

    # # Retry settings
    # "RETRY_ENABLED": True,
    # "RETRY_TIMES": 3,                # retry failed requests up to 3 times
    # }

    def __init__(self, *args, **kwargs):
        super(UFCFightSpider, self).__init__(*args, **kwargs)
        self.fight_data = {key: [] for key in self.fight_fields}
        self.events_to_scrape = set()  # Store event links to scrape

        # fights are not scraped in order?

    def check_html(self, link):
        return link.endswith('.html')

    # def start_requests(self):
    #     # Only retry fights that failed previously
    #     for fight_url in all_failed_fights:
    #         yield scrapy.Request(url=fight_url, callback=self.parse_fight_ordered, meta={'fight_url': fight_url}, headers={'User-Agent': 'Mozilla/5.0'})

    def parse(self, response):
        # fights are not scraped in order?

        # if len(all_failed_fights)>1:
        #     for fight_url in all_failed_fights:
        #         if self.check_html(fight_url+'.html') and "fight-details" in fight_url:
        #             yield response.follow(fight_url, callback=self.parse_fight_ordered, meta={'fight_url': fight_url}, headers={'User-Agent': 'Mozilla/5.0'})

    
        # Select event links
        events_completed = response.css('tr.b-statistics__table-row td i a::attr(href)').getall()


        for link in events_completed:
            if link not in self.events_to_scrape:
                self.events_to_scrape.add(link)


        self.logger.info(f"Found {len(self.events_to_scrape)} new events to scrape")

        #for each event link check if its a real link then proceed to parse_event to extract details of all fights
        for event_url in self.events_to_scrape:
            if self.check_html(event_url+'.html') and 'http://ufcstats.com/event-details/' in event_url:
                yield response.follow(event_url, callback=self.parse_event_ordered, meta={'event_url': event_url}, headers={'User-Agent': 'Mozilla/5.0'})



    def parse_event_ordered(self, response):
        fight_links = response.css('tr[data-link]::attr(data-link)').getall()
        fight_links = [link for link in fight_links if link not in scraped_fights_set and link not in all_failed_fights]

        self.logger.info(f"Found {len(fight_links)} fights in event {response.url}")

        for fight_url in fight_links:
            if self.check_html(fight_url+'.html') and "fight-details" in fight_url:
                yield response.follow(fight_url, callback=self.parse_fight_ordered, meta={'fight_url': fight_url},headers={'User-Agent': 'Mozilla/5.0'})



    def parse_fight_ordered(self, response):
        fight_url = response.meta.get('fight_url', response.url)
        
        # Revised extraction with better error handling
        try:

            # Extract winner
            winner = None
            winner_i = response.css('div.b-fight-details__person i.b-fight-details__person-status.b-fight-details__person-status_style_green')
            if winner_i:

                winner_container = winner_i.xpath('parent::div[contains(@class,"b-fight-details__person")]')
                winner_tag = winner_container.css('div h3 a::text').get()
                winner = winner_tag.strip() if winner_tag else None
            else:
                # Check for NC or D results
                gray_status = response.css('div.b-fight-details__person i.b-fight-details__person-status.b-fight-details__person-status_style_gray::text').get().strip()
                if gray_status in ['NC', 'D']:
                    winner = gray_status
                else:
                    self.logger.error(f"Error parsing winner for fight {response.url}")
                    winner = "Unknown"

        

    #after debugging there are some fights without stats even though they have outcomes, 
    # many fights are scraped with correct links and outcomes but wrong other stats
            # Extract fight stats table - the second 'tr' element (index 1)
        
            fight_stats_tr = response.css('tr')[1]

            # if not fight_stats_tr:
            #     self.logger.warning(f"No fight stats table found for fight {response.url}")
            #     # Append Nones for all fields except winner to keep lengths consistent
            #     for key in self.fight_fields:
            #         if key != 'Winner':
            #             self.fight_data[key].append(None)
            #     return

            tds = fight_stats_tr.css('td')

            #terrible matching
            fighterA=tds[0].css('p a::text').get(default='').strip()
            
            


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

            self.fight_data['Winner'].append(winner)

            self.fight_data['linktofight'].append(fight_url)
            scraped_fights_set.add(fight_url)
            all_failed_fights.discard(fight_url)

        except Exception as e:
            # the first table is there but maybe there is a difference in the format
            all_failed_fights.add(fight_url)
            self.logger.error(f"Error parsing fight stats for {response.url}: {e}")

            # # Append None to all fields except Winner to maintain consistent list lengths
            # for key in self.fight_fields:
            #     if key != 'Winner' and len(self.fight_data[key]) < len(self.fight_data['Winner']):
            #         self.fight_data[key].append(None)




    def closed(self, reason):
        import pandas as pd
        for column in self.fight_data:
            print(column)
            print('length '+str(len(self.fight_data[column])))

        number_of_fights=min(len(values) for values in self.fight_data.values())
        self.fight_data={k: v[:number_of_fights] for k, v in self.fight_data.items()}


            # Load existing scraped fights if the file exists
        if os.path.exists("ufc_fights.xlsx"):
            existing_df = pd.read_excel("ufc_fights.xlsx")
            # Convert to dict of lists
            existing_data = {col: existing_df[col].tolist() for col in existing_df.columns}
        else:
            existing_data = {key: [] for key in self.fight_fields}

        # Merge new fight_data at the top of existing data
        merged_data = {}
        for key in self.fight_fields:
            merged_data[key] = self.fight_data[key] + existing_data.get(key, [])

        #i ran the code the first time it took the 8303 fights, i ran it correctly failed to scrape 21 fights that dont have stats only outcomes
        # Convert to DataFrame and save
        df = pd.DataFrame(merged_data)
        df.to_excel("ufc_fights.xlsx", index=False)
        self.logger.info(f"Saved {len(self.fight_data['linktofight'])} new fights on top of existing {len(existing_data['linktofight'])} fights")

        
        
        # Save scraped fights and events JSON
        with open(SCRAPED_FIGHTS_FILE, 'w') as f:
            json.dump(list(scraped_fights_set), f)

        # with open(SCRAPED_EVENTS_FILE, 'w') as f:
        #     json.dump(list(scraped_events_set.union({link for link in self.events_to_scrape})), f)

        # Update unscraped/failed fights JSON
        with open(UNSCRAPED_FIGHTS_FILE, 'w') as f:
            json.dump(list(all_failed_fights), f, indent=2)



from scrapy.crawler import CrawlerProcess
import time
start_time = time.time()

process = CrawlerProcess(settings={
    "LOG_LEVEL": "DEBUG",  # or "DEBUG" for even more detail
    'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter'
})
process.crawl(UFCFightSpider)
process.start()
print("Scraping completed.")
print(f"Total execution time: {time.time() - start_time:.2f} seconds")
