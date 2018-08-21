import json
import requests
import threading
from queue import Queue
from pyquery import PyQuery as pq

# 控制线程退出
CRAWL_EXIT = False
PARSE_EXIT = False


class ThreadCrawl(threading.Thread):
    """
    爬取多线程类
    """

    def __init__(self, thread_name, page_queue, data_queue):
        # 继承父类
        super(ThreadCrawl, self).__init__()
        # 线程名字
        self.thread_name = thread_name
        # 页码队列
        self.page_queue = page_queue
        # 数据队列
        self.data_queue = data_queue
        # 请求头
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                                      "Chrome/67.0.3396.99 Safari/537.36"}

    def run(self):
        while not CRAWL_EXIT:
            try:
                # 取出一个数字，先进先出
                # 可选参数block，默认值为True
                # -如果队列为空，block为True的话，不会结束，会进入阻塞状态，直到队列有新的数据
                # -如果队列为空，block为False的话，就弹出一个Queue.empty()异常
                page = self.page_queue.get(False)
                url = "https://www.qiushibaike.com/textnew/page/" + str(page) + "/"
                print("开始%s,页码：%d,url=%s" % (self.thread_name, page, url))
                html = requests.get(url, headers=self.headers).text
                # 将源码放进队列
                self.data_queue.put(html)
            except:
                pass


class ThreadParse(threading.Thread):
    """
    分析多线程类
    """
    def __init__(self, thread_name, data_queue, filename, lock):
        super(ThreadParse, self).__init__()
        self.thread_name = thread_name
        self.data_queue = data_queue
        self.filename = filename
        self.lock = lock

    def run(self):
        print("开始%s" % self.thread_name)
        while not PARSE_EXIT:
            try:
                html = self.data_queue.get(False)
                self.parse(html)
            except:
                pass

    def parse(self, html):
        doc = pq(html)
        items = doc('.col1 .article').items()
        data = {}
        for item in items:
            data['text'] = ''.join(item.find('.content').text().split())
            data['author'] = item.find('div.author.clearfix > a:nth-child(2)').text()
            data['Funny'] = ''.join(item.find('div.stats > span.stats-vote').text().split())
            data['comments'] = ''.join(item.find('div.stats > span.stats-comments').text().split())
            print(data)

            # 保存到json文件
            with self.lock:
                self.filename.write(json.dumps(data, ensure_ascii=False))
                self.filename.write('\n')


def main():
    # 创建线程锁
    lock = threading.Lock()

    # 页码队列，表示35个页面
    page_queue = Queue(35)
    # 在页码队列中放入1-35，先进先出
    for i in range(1, 36):
        page_queue.put(i)

    # 数据队列，参数为空表示不限制
    data_queue = Queue()

    # 创建文件
    filename = open("糗事百科.json", "a", encoding='utf8')

    # 三个采集线程名字
    thread_names = ['采集线程1', '采集线程2', '采集线程3']
    # 将采集线程存储
    thread_crawl = []
    for thread_name in thread_names:
        crawl = ThreadCrawl(thread_name, page_queue, data_queue)
        crawl.start()
        thread_crawl.append(crawl)

    # 三个解析线程名字
    thread_names = ['解析线程1', '解析线程2', '解析线程3']
    # 将解析线程存储
    thread_parse = []
    for thread_name in thread_names:
        parse = ThreadParse(thread_name, data_queue, filename, lock)
        parse.start()
        thread_parse.append(parse)

    while not page_queue.empty():
        pass
    # 采集线程结束，退出
    global CRAWL_EXIT
    CRAWL_EXIT = True

    while not data_queue.empty():
        pass
    # 解析线程结束，退出
    global PARSE_EXIT
    PARSE_EXIT = True

    # 等待采集线程结束
    for parse in thread_parse:
        parse.join()
        print("%s线程结束" % str(parse))

    with lock:
        filename.close()


if __name__ == '__main__':
    main()
