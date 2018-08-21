import requests
import threading
from queue import Queue
from pyquery import PyQuery as pq

# 控制线程结束
CRAWL_EXIT = False
PARSE_EXIT = False
IMAGE_EXIT = False

# 保存的图片文件夹位置
path = 'E:/doutu/'
num = 1001


class ThreadCrawl(threading.Thread):
    """
    爬取的多线程类
    """

    def __init__(self, crawl_name, page_queue, data_queue):
        super(ThreadCrawl, self).__init__()
        self.crawl_name = crawl_name
        self.page_queue = page_queue
        self.data_queue = data_queue
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                                      "Chrome/67.0.3396.99 Safari/537.36"}

    def run(self):
        while not CRAWL_EXIT:
            try:
                page = self.page_queue.get(False)
                url = "https://www.doutula.com/photo/list/?page=" + str(page)
                print("%s开始爬取第%d页，url=%s" % (self.crawl_name, page, url))
                html = requests.get(url, headers=self.headers).text
                self.data_queue.put(html)
            except:
                pass


class ThreadParse(threading.Thread):
    """
    解析的多线程类
    """

    def __init__(self, parse_name, data_queue, image_queue):
        super(ThreadParse, self).__init__()
        self.parse_name = parse_name
        self.data_queue = data_queue
        self.image_queue = image_queue

    def run(self):
        while not CRAWL_EXIT:
            try:
                print("启动%s" % (self.parse_name))
                # 获取网页源码
                html = self.data_queue.get(False)
                # 解析网页源码
                self.parse(html)
            except:
                pass

    def parse(self, html):
        doc = pq(html)
        items = doc("#pic-detail > div > div.col-sm-9 > div.random_picture > ul > li > div > div > a").items()
        for item in items:
            image = item.find('img').attr('data-original')
            image = str(image).replace('.jpg', '')
            name = item.find('p').text()
            print(str(image) + str(name))
            if image != 'None':
                self.image_queue.put(image)


class ThreadImage(threading.Thread):
    """
    抓取到的图片的多线程处理类
    """

    def __init__(self, save_name, image_queue, lock):
        super(ThreadImage, self).__init__()
        self.save_name = save_name
        self.image_queue = image_queue
        self.lock = lock

    def run(self):
        while not IMAGE_EXIT:
            try:
                print("启动%s" % (self.save_name))
                image_url = self.image_queue.get(False)
                self.save(image_url)
            except:
                pass

    def save(self, image_url):
        global num
        result = requests.get(image_url).content
        filename = path + str(num) + '.jpg'
        with open(filename, 'wb') as f:
            f.write(result)
            f.flush()
            num += 1


def main():
    # 创建页码队列,爬取10页
    page_queue = Queue(10)
    for i in range(1, 11):
        page_queue.put(i)

    # 创建图片链接队列
    image_queue = Queue()

    # 创建数据队列
    data_queue = Queue()

    # 创建锁
    lock = threading.Thread()

    # 三个采集线程名
    crawl_names = ['采集线程1', '采集线程2', '采集线程3']
    # 存储采集线程
    crawl_list = []
    for crawl_name in crawl_names:
        crawl = ThreadCrawl(crawl_name, page_queue, data_queue)
        crawl.start()
        crawl_list.append(crawl)

    # 三个解析线程名
    parse_names = ['解析线程1', '解析线程2', '解析线程3']
    # 保存解析线程
    parse_list = []
    for parse_name in parse_names:
        parse = ThreadParse(parse_name, data_queue, image_queue)
        parse.start()
        parse_list.append(parse)

    # 三个保存图片线程名
    save_names = ['保存图片1', '保存图片2', '保存图片3']
    # 存储图片线程
    image_list = []
    for save_name in save_names:
        image = ThreadImage(save_name, image_queue, lock)
        image.start()
        image_list.append(image)

    # 采集线程结束，退出
    while not page_queue.empty():
        pass
    global CRAWL_EXIT
    CRAWL_EXIT = True

    # 解析线程结束，退出
    while not data_queue.empty():
        pass
    global PARSE_EXIT
    PARSE_EXIT = True

    # 存储图片线程结束，退出
    while not image_queue.empty():
        pass
    global IMAGE_EXIT
    IMAGE_EXIT = True


if __name__ == '__main__':
    main()
