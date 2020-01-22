from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://{user}:{pw}@{url}/{db}".format(
    user="",
    pw="", 
    url="",
    db=""))