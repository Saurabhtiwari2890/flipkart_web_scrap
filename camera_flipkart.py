from bs4 import BeautifulSoup
import pandas as pd
import requests
import urllib2
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
auth_provider = PlainTextAuthProvider(
        username='cassandra', password='cassandra')
cluster = Cluster(auth_provider=auth_provider)
session = cluster.connect('flipkart')

m_links = []

# url = raw_input('Type the url of Website:')
url='http://www.flipkart.com/cameras/pr?sid=jek%2Cp31&layout=grid&otracker=ch_vn_camera_filter_Camera+Brands_All&ajax=true&start'
request = requests.get(url)
data = request.text
soup = BeautifulSoup(data,'lxml')
data = soup.findAll('div')
for link in soup.findAll('ul',attrs={'id':'brand'}):
     for j in link.find_all('a'):
        m_links.append(j.get('href'))

prd_links = []

for i in range(0,len(m_links)):
    m_links[i]= 'http://www.flipkart.com'+m_links[i]+'&ajax=true&start'
    request = requests.get(m_links[i])
    data = request.text
    soup = BeautifulSoup(data,'lxml')
    # data = soup.findAll('div',attrs={'class':'pu-final'})
    for x in soup.findAll('div',attrs={'id':'products'}):
        for y in soup.findAll(attrs={'class':'fk-display-block'}):
            for z in soup.findAll('a'):
                if y.get('href'):
                    y.get('href')
                    prd_links.append(y.get('href'))
                    break

data_list=[]
for i in range(0,len(prd_links)):
    prd_links[i]= 'http://www.flipkart.com'+prd_links[i]
    request = requests.get(prd_links[i])
    data = request.text
    soup = BeautifulSoup(data, 'lxml')
    if soup.findAll('h1',attrs={"itemprop":"name"}):
        p_name = soup.findAll('h1',attrs={"itemprop":"name"})
        p_name =str(p_name)
        p_name = BeautifulSoup(p_name,'lxml').text.encode('utf-8')

    else:
        p_name = "Name N/A"
    if soup.findAll("span",attrs={"class":"selling-price omniture-field"}):
        p_price =soup.findAll("span",attrs={"class":"selling-price omniture-field"})
        p_price =str(p_price)
        p_price = BeautifulSoup(p_price,'lxml').text.encode('utf-8')

    else:
        p_price = "Price N/A"
    if soup.findAll('div',attrs={'class':"bigStar"}):
        p_ratings = soup.findAll('div',attrs={'class':"bigStar"})
        p_ratings =str(p_ratings)
        p_ratings = BeautifulSoup(p_ratings,'lxml').text.encode('utf-8')

    else:
        p_ratings =  "Rating N/A"
    data = dict(Product_Name=p_name,Product_Price=p_price,Product_Rating=p_ratings)
    data_list.append(data)

df = pd.DataFrame(data_list)



for index, row in df.iterrows():

    stmt = session.prepare("INSERT INTO Camera(product_name,product_price,product_rating) VALUES (?, ?, ?) IF NOT EXISTS")
    results = session.execute(stmt, row)
    print row
