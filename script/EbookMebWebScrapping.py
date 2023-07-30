from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from pandas import pandas as pd

titles = []
book_names = []
writers = []
ratings = []
values = []

options = Options()
options.headless = True
options.add_argument("--window-size=1920,1200")

CHROME_DRIVER_PATH = r'C:\Users\Khanoon\Desktop\chromedriver_win32\chromedriver.exe'

service = Service(executable_path='./chromedriver.exe')
options = webdriver.ChromeOptions()
driver = webdriver.Chrome(service=service, options=options)

# for i in range(1,2):
for i in range(1,25):

    url = f"https://www.mebmarket.com/index.php?store=category&action=book_list&condition=paid&category_id=15&category_name=%E0%B8%9E%E0%B8%B1%E0%B8%92%E0%B8%99%E0%B8%B2%E0%B8%95%E0%B8%99%E0%B9%80%E0%B8%AD%E0%B8%87&page_no={i}"
    driver.get(url)

    elements = driver.find_elements(By.CLASS_NAME, 'book_listing')

    for i in elements:
        title = i.find_element(By.CLASS_NAME, 'book_name').text.replace(',', '')
        titles.append(title)
        writer = i.find_element(By.CLASS_NAME, 'book_meta_info').text
        writers.append(writer)
        rating = i.find_element(By.CLASS_NAME, 'text_rating_book_list').text
        ratings.append(rating)
        buy_button = i.find_element(By.CLASS_NAME, 'table_btn_book_list').find_element(By.CSS_SELECTOR, "input[value*='฿']")
        value = buy_button.get_attribute("value")
        values.append(value)

driver.quit()

# Create a DataFrame
data = {'title': titles, 'writer' : writers, 'value': values, 'rating' : rating, 'platform' : 'MEB'}
df_meb = pd.DataFrame(data)

df_meb['value'] = df_meb['value'].str.replace(',', '').str.replace('฿ ', '').astype(float)
df_meb['rating'] = df_meb['rating'].str.replace(' Rating', '').astype(int)

# df_meb.insert(0, 'datetime', pd.to_datetime('now'))

# Output the DataFrame
print(df_meb.iloc[:,2:])

csv_file_path = r"C:\Users\Khanoon\Desktop\chromedriver_win32\book_details_meb.csv"
df_meb.to_csv(csv_file_path, index=False)