import logging
import random
import pika
import os
import time
import json
from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
from datetime import datetime
import math

JOBS_EXCHANGE = "jobs_exchange"
RESULTS_EXCHANGE = "results_exchange"

# Pobieranie hosta RabbitMQ z ENV
RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
# Unikalny identyfikator workera (z ENV lub losowy)
WORKER_ID = os.getenv('WORKER_ID', f"worker-{random.randint(1000, 9999)}")
# Konfiguracja loggera
logging.basicConfig(level=logging.INFO, format=f'%(asctime)s [{WORKER_ID}] : %(message)s')
logger = logging.getLogger(__name__)

class Worker:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.session = requests.Session()
        self.target_url = ""

    def get_random_link(self, soup, url):
        links = soup.select('a')
        valid_links = []
        for link in links:
            href = link.get('href')
            if href and not href.startswith('http') and not href.startswith('#'):
                valid_links.append(href)
            elif href and href.startswith(url):
                valid_links.append(href)
        
        if valid_links:
            return urljoin(url, random.choice(valid_links))
        return None

    def perform_request(self, method, url, data=None, timeout=5.0, scenario_step=0):
        start_time = time.time()
        error_msg = None
        status_code = 0
        response_size = 0
        
        try:
            # Użycie timeout z wiadomości IN
            if method == 'GET':
                response = self.session.get(url, timeout=timeout)
            else:
                response = self.session.post(url, data=data, timeout=timeout)
            
            status_code = response.status_code
            response_size = len(response.content) # Liczymy bajty
            ttfb_ms = response.elapsed.total_seconds() * 1000
            
        except requests.exceptions.RequestException as e:
            # Obsługa błędów sieciowych (nie HTTP)
            logger.error(f"Request failed: {e}")
            error_msg = str(e)
            status_code = 0 
            response = None
            
        latency = (time.time() - start_time) * 1000
        path = url.replace(self.target_url, '') if self.target_url in url else url
        result = {
            'worker_id': WORKER_ID,
            'timestamp': datetime.now().isoformat(),
            'method': method,
            'endpoint': path,
            'status_code': status_code,
            'latency_ms': round(latency, 2),
            'ttfb_ms': round(ttfb_ms, 2),
            'response_size_bytes': response_size,
            'error_msg': error_msg,
            'is_success': (200 <= status_code < 300) and (error_msg is None),
            'scenario_step': scenario_step
        }

        self.send_result(method, result)
        return response, status_code

    def send_result(self, result):
        if self.channel:
            self.channel.basic_publish(
                exchange='', 
                routing_key=RESULTS_EXCHANGE, 
                body=json.dumps(result)
            )

    def calculate_lognorm_params(self, desired_mean, desired_std_dev):
        """
        Przelicza oczekiwaną średnią (np. 4 sekundy) i odchylenie (rozrzut)
        na parametry mu i sigma wymagane przez rozkład Log-Normalny.
        """
        # Wzory matematyczne na konwersję momentów:
        # sigma^2 = ln(1 + (Var / Mean^2))
        # mu = ln(Mean) - 0.5 * sigma^2
        
        variance = desired_std_dev ** 2
        sigma_sq = math.log(1 + (variance / (desired_mean ** 2)))
        
        sigma = math.sqrt(sigma_sq)
        mu = math.log(desired_mean) - 0.5 * sigma_sq
        
        return mu, sigma

    def think(self, avg_time, std_dev):
        """
        Symuluje czas namysłu używając rozkładu Log-Normalnego.
        avg_time: Średni czas oczekiwania w sekundach (oczekiwany przez użytkownika).
        std_dev: Odchylenie standardowe czasu oczekiwania w sekundach.
        """
        if avg_time <= 0: return

        # Obliczamy matematyczne parametry rozkładu
        mu, sigma = self.calculate_lognorm_params(avg_time, std_dev)
        
        # Losujemy czas
        wait_time = random.lognormvariate(mu, sigma)
        
        # Opcjonalnie: Limit górny (Hard cap), żeby worker nie zasnął na godzinę
        # przy skrajnie rzadkim wylosowaniu z "ogona" rozkładu.
        wait_time = min(wait_time, avg_time * 10) 

        logger.info(f"Myślę... {wait_time:.2f}s")
        time.sleep(wait_time)

    def run_crawl_session(self, config): 
        timeout = config.get('timeout', 2.0)
        think_time_avg = config.get('think_time_avg', 2.0)
        think_time_var = config.get('think_time_var', 0.5)
        self.target_url = config.get('target_url', 'http://localhost:8080')
        user_agent = config.get('user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3')
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept-Language': 'pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7', 
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
        })
        
        logger.info(f"Ustawiono User-Agent: {user_agent[:30]}...")
        session_depth = config.get('depth', 1)
        logger.info(f"--- NOWA SESJA (Głębokość: {session_depth}) ---")

        current_url = self.target_url

        for step in range(session_depth):
            response, status = self.perform_request('GET', current_url, timeout=timeout, scenario_step=step)
            
            if not response or status != 200:
                break

            if step == session_depth - 1:
                break
                
            self.think(think_time_avg, think_time_var)
            
            soup = BeautifulSoup(response.text, 'html.parser')
            next_url = self.get_random_link(soup, current_url)
            
            if next_url:
                current_url = next_url
            else:
                break


    def start(self):
        def callback(ch, method, properties, body):
            cfg = json.loads(body)
            payload = body.decode("utf-8", errors="replace")
            routing_key = method.routing_key

            logger.info(f"[x] Received job via {routing_key}: {payload}")

            start_delay = cfg.get('start_delay', 0)
                    
            if start_delay > 0:
                logger.info(f"Otrzymano zadanie. Oczekiwanie na start (Ramp-up): {start_delay}s...")
                time.sleep(start_delay)
            else:
                logger.info("Otrzymano zadanie. Start natychmiastowy.")
            # ---------------------------------------------------

            self.run_crawl_session(cfg)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("Zadanie zakończone. Czekam na kolejne.")
        #---------------------------------------------------
        while True:
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=RABBIT_HOST)
                )
                break
            except pika.exceptions.AMQPConnectionError:
                print(f"[!] Cannot connect to RabbitMQ at {RABBIT_HOST}, retrying in 5s...")
                time.sleep(5)

        self.channel = connection.channel()

        self.channel.exchange_declare(exchange=JOBS_EXCHANGE, exchange_type="topic", durable=True)
        self.channel.exchange_declare(exchange=RESULTS_EXCHANGE, exchange_type="topic", durable=True)

        self.channel.queue_declare(queue="jobs_queue", durable=True)
        self.channel.queue_bind(
            exchange=JOBS_EXCHANGE,
            queue="jobs_queue",
            routing_key="job.*"
        )

        print(" [*] Python worker waiting for jobs...")
        self.channel.basic_consume(queue="jobs_queue", on_message_callback=callback)

        self.channel.start_consuming()

if __name__ == "__main__":
    worker = Worker()
    worker.start()
