# from urllib import request
import urllib.request
import re
from pymongo import MongoClient
import random
from bs4 import BeautifulSoup
from datetime import datetime
import time
from py2neo import Graph, Node, Relationship
from py2neo import Subgraph
from pandas import DataFrame

class Conference_Spider(object):
    def __init__(self):
        self.url_head = 'http://www.wikicfp.com/cfp/call?conference='
        self.url_tail = '&skip='
        self.conference_type = ['computer%20science',
                                'machine%20learning',
                                'artificial%20intelligence',
                                'NLP']
        self.usrAgent = [
            "Mozilla/5.0 (Linux; Android 4.1.1; Nexus 7 Build/JRO03D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Safari/535.19",
            "Mozilla/5.0 (Linux; U; Android 4.0.4; en-gb; GT-I9300 Build/IMM76D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30",
            "Mozilla/5.0 (Linux; U; Android 2.2; en-gb; GT-P1000 Build/FROYO) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
            "Mozilla/5.0 (Windows NT 6.2; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0",
            "Mozilla/5.0 (Android; Mobile; rv:14.0) Gecko/14.0 Firefox/14.0",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36",
            "Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.133 Mobile Safari/535.19",
            "Mozilla/5.0 (iPad; CPU OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3",
            "Mozilla/5.0 (iPod; U; CPU like Mac OS X; en) AppleWebKit/420.1 (KHTML, like Gecko) Version/3.0 Mobile/3A101a Safari/419.3"]

        self.header = {'Host': 'www.wikicfp.com',
                       'Cookie':''}

        self.month_dict = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6, 'July': 7,
                           'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12,
                           'JANUARY': 1, 'FEBUARY': 2, 'MARCH': 3, 'APRIL': 4, 'JUNE': 6, 'JULY': 7,
                           'AUGUST': 8, 'SEPTEMBER': 9, 'OCTOBER': 10, 'NOVEMBER': 11, 'DECEMBER': 12,
                           'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6, 'JUL': 7,
                           'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
                           'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'Jun': 6, 'Jul': 7,
                           'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}

        host = ''
        port = ''
        user_name = ''
        user_pwd = ''
        db_name = ''
        uri = "mongodb://" + user_name + ":" + user_pwd + "@" + host + ":" + port + "/" + db_name
        client = MongoClient(uri)
        self.db = client[db_name]

    def get_ip_list(self,obj):
        ip_text = obj.findAll('tr', {'class': 'odd'})
        ip_list = []
        for i in range(len(ip_text)):
            ip_tag = ip_text[i].findAll('td')
            ip_port = ip_tag[1].get_text() + ':' + ip_tag[2].get_text()
            ip_list.append(ip_port)
        # print("共收集到了{}个代理IP".format(len(ip_list)))
        # print(ip_list)
        return ip_list

    def get_random_ip(self,bsObj):
        ip_list = self.get_ip_list(bsObj)
        random_ip = 'http://' + random.choice(ip_list)
        self.proxy = {'http:': random_ip}
        # print('check point: get_proxy')

    def get_proxy(self):
        '''
        only run once for a day, save a self.bsObjct for proxy pool
        :return:
        '''
        url = 'http://www.xicidaili.com/nn'
        headers = {}
        headers["User-Agent"] = self.random_select_header(self.usrAgent)
        headers["Upgrade-Insecure-Requests"] = 1
        headers["Accept-Language"] = 'zh-cn'
        headers["Connection"] = 'keep-alive'
        headers["Accept"] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        headers["Host"] = 'www.xicidaili.com'
        headers["Referer"] = 'www.xicidaili.com'
        request = urllib.request.Request(url,headers=headers)
        response = urllib.request.urlopen(request)
        bsObj = BeautifulSoup(response, 'lxml')
        return bsObj

    def create_url(self,confernece_type, page=1):
        return self.url_head + confernece_type + self.url_tail + str(page)

    def random_select_header(self,list):
        agent = random.choice(list)
        return agent

    def get_data(self,url):
        # proxy_support = urllib.request.ProxyHandler(self.proxy)
        # opener = urllib.request.build_opener(proxy_support)
        # urllib.request.install_opener(opener)
        self.header["User-Agent"] = self.random_select_header(self.usrAgent)
        self.request = urllib.request.Request(url=url, headers=self.header)
        self.response = urllib.request.urlopen(self.request, timeout=20)
        html = self.response.read()
        self.html = str(html, 'utf-8')
        # print('check point get_data')
        # print(self.html)

    def get_page_num(self):
        soup = BeautifulSoup(self.html, 'lxml')
        data = soup.select('tr tr td[align="center"]')[-1]
        data = re.sub(r'<[^>]+>','',str(data))
        # print(data)
        page_num = re.findall(r'\s[0-9]{2,3}\s',data)
        # print(page_num)
        page_num = int(re.sub(r'\s','',str(page_num[0])))
        return page_num

    def data_preprocessing(self,discipline):
        soup = BeautifulSoup(self.html,'lxml')
        data_list = soup.select('td table tr')

        for i in data_list:
            raw_data = i.select('td a')
            if raw_data != []:
                link = re.findall(r'"[^>]+"',str(raw_data[0]))
                name = re.sub(r'<[^>]+>','',str(raw_data[0]))
                if len(link) == 1 and 'http' not in link:
                    if 'About Us' not in name and 'first' not in name and 'License' not in name:

                        link = re.sub(r'"','',str(link[0]))
                        link = 'http://www.wikicfp.com' + link

                        # print(link)
                        # print(name)
            raw_data_info = i
            info = raw_data_info.select('td[align="left"]')
            if len(info) == 3:
                info = [re.sub(r'<[^>]+>','',str(i)) for i in info]
                if '\n' not in info[0]:
                    duration = info[0]
                    location = info[1]
                    ddl = self.decode(info[2])
                    # print(duration)
                    # print(location)
                    # print(ddl)
                    if name != None:
                        if link != None:
                            self.save_data(name,link,duration,location,ddl,discipline)

    def save_data(self,name,link,duration,location,ddl,discipline):
        exist = self.db.Conference.find({"link":link}).count()
        if exist == 0:
            self.db.Conference.insert({'name':name,
                                           'link':link,
                                           'duration':duration,
                                           'location':location,
                                           'ddl':ddl,
                                           'discipline':discipline})
            print('The conference %s saved' %str(name))
        else:
            print('The conference %s exists' %str(name))

    def decode(self, t):
        data = ''
        mth = ''
        day = ''
        year = ''
        for i in range(len(t)):
            if t[i] not in [' ', ',']:
                data += t[i]
            elif data != '':
                if mth == '':
                    mth = str(self.month_dict[data])
                    if len(mth) < 2:
                        mth = '0' + mth
                elif day == '':
                    day = data
                    if len(data) < 2:
                        day = '0' + data
                elif year == '':
                    year = data
                    break
                data = ''
        if year == '':
            year = data
        time = year + '-' + mth + '-' + day
        return time

    def _format_transfer(self,str):
        return re.sub('%20',' ',str)

    def _location_to_country(self,str):
        st = str.split(',')
        string = re.sub(r'\s','',st[-1])
        return string

    def kg_save(self):

        data = self.db.Conference.find({})
        dict = [i for i in data]
        # print(dict[0])
        graph = Graph('', user='', password = '')
        graph.delete_all()
        tx = graph.begin()
        for i in dict:
            conference = Node('Conference',name=i["name"])
            conference['ddl'] = i['ddl']
            location = Node('Location',name=self._location_to_country(i["location"]))
            about = Node('Discipline',name=self._format_transfer(i['discipline']))
            tx.merge(conference,primary_label='Conference',primary_key='name')
            tx.merge(location,primary_label='Location',primary_key='name')
            tx.merge(about,primary_label='Discipline',primary_key='name')
            rel_about = Relationship(about,'has_meeting', conference)
            rel_loc = Relationship(conference,'at',location)
            rel_loc['duration'] = i['duration']
            tx.merge(rel_about)
            tx.merge(rel_loc)
            print('{} is saved'.format(rel_about))
            print('{} is saved'.format(rel_loc))

        tx.commit()

    def get_kg_data(self):
        graph = Graph('', user='', password='')
        data = graph.match(nodes='Location')

        print(data)

    def kg_test(self):
        a = Node("Person", name="Alice")

        b = Node("Person", name="Bob")

        c = Node("Person", name="Caroal")

        ab = Relationship(a, "KNOWS", b)
        ac = Relationship(a, "HATES", c)
        x = ab|ac
        graph = Graph('', user='', password='')

        graph.create(x)

if __name__ == "__main__":
    C = Conference_Spider()
    conference_list = C.conference_type
    # C.kg_test()
    C.kg_save()
    C.get_kg_data()
    # for j in range(10):
    #     Obsj = C.get_proxy()
    #     C.get_random_ip(Obsj)
    #     if j >=1:
    #         i = conference_list[0]
    #
    #         url = C.create_url(i, j)
    #         # print(url)
    #         C.get_data(url)
    #         C.data_preprocessing(i)
    #     time.sleep(5)
    #
    # for j in range(10):
    #     Obsj = C.get_proxy()
    #     C.get_random_ip(Obsj)
    #     if j >=1:
    #
    #         i = conference_list[1]
    #
    #         url = C.create_url(i, j)
    #         # print(url)
    #         C.get_data(url)
    #         C.data_preprocessing(i)
    #     time.sleep(5)

    # for j in range(10):
    #     # Obsj = C.get_proxy()
    #     # C.get_random_ip(Obsj)
    #     if j >=1:
    #         i = conference_list[2]
    #
    #         url = C.create_url(i, j)
    #         # print(url)
    #         C.get_data(url)
    #         C.data_preprocessing(i)
    #     time.sleep(5)
    #
    # for j in range(10):
    #     # Obsj = C.get_proxy()
    #     # C.get_random_ip(Obsj)
    #     if j >=1:
    #         i = conference_list[3]
    #
    #         url = C.create_url(i, j)
    #         # print(url)
    #         C.get_data(url)
    #         C.data_preprocessing(i)
    #     time.sleep(5)
    #
    # C.kg_save()