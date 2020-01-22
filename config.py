from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://{user}:{pw}@{url}/{db}".format(
    user="martin",
    pw="$coder238", 
    url="mthodi.mysql.pythonanywhere-services.com",
    db="mthodi$miyanda_db"))