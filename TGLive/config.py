from os import getenv, path
from dotenv import load_dotenv

load_dotenv(path.join(path.dirname(path.dirname(__file__)), "config.env"))

class Telegram:
    API_ID = int(getenv("API_ID", "11405252"))
    API_HASH = getenv("API_HASH", "b1a1fc3dc52ccc91781f33522255a880")
    BOT_TOKEN = getenv("BOT_TOKEN", "6326333011:AAHFsd404duVRtBJpFtKEGlWZkT14mUwQCM")
    
    HELPER_BOT_TOKEN = getenv("HELPER_BOT_TOKEN", "6441552101:AAELtzqFk9L-jFocRx1bRLqV3N0tgvwfb-U")

    BASE_URL = getenv("BASE_URL", "").rstrip('/')
    PORT = int(getenv("PORT", "8000"))
    DATABASE_URL = getenv("DATABASE_URL", "mongodb+srv://sibuna123:sibuna123@personalproject.rb8q7.mongodb.net")
    # POSTGRES_URL = getenv("POSTGRES_URL", "postgresql://neondb_owner:npg_xG9uCmRUTOQ0@ep-royal-bar-a4s9a4y7-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require")
    POSTGRES_URL = getenv("POSTGRES_URL", "postgresql://neondb_owner:npg_xG9uCmRUTOQ0@ep-lively-bonus-a4juxhe2-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require")


    UPSTREAM_REPO = getenv("UPSTREAM_REPO", "")
    UPSTREAM_BRANCH = getenv("UPSTREAM_BRANCH", "")

    OWNER_ID = int(getenv("OWNER_ID", "1881720028"))
    CHANNEL_ID = int(getenv("CHANNEL_ID", "-1001719899162"))
    DB_CHANNEL_ID = int(getenv("DB_CHANNEL_ID", "-1003523724632"))
    FAMILY_GUY_ID = int(getenv("FAMILY_GUY_ID", "-1002037296945"))
    BEN_ID = int(getenv("BEN_ID", "-1003513249533"))
    DORAEMON_ID = int(getenv("DORAEMON_ID", "-1003517298644"))
    NARUTO_ID = int(getenv("NARUTO_ID", "-1003533249597"))
    NATIA_ID = int(getenv("NATIA_ID", "-1003551915792"))
    PANDF_ID = int(getenv("PANDF_ID", "-1003577274228"))
    LOCAL_ID = int(getenv("LOCAL_ID", "-1003613880985"))
    
    MULTI_TOKEN1="8461823650:AAFGAtLCt-9Nn1sLZ73G0slCFm6wySrfZmM"
    MULTI_TOKEN2="8379790965:AAFVLnEcB2nTCpYoZ50jevejPIWXggrciQ0"
    MULTI_TOKEN3="8277438394:AAFWUh-2ykWZzK82YDlgYW9pM_4_uzajIRE"
    MULTI_TOKEN4="8053283698:AAEhmFcztGIEvp4DufQ4s4Gf-iy9X4sW8kE"
    MULTI_TOKEN5="8468907864:AAHYC9q1PbbavJii1ufriHCgmK1VY1P20Hk"
    MULTI_TOKEN6="8424078653:AAEZDHBs54aVP7QzR3KXM-FGn4Qm8w7OE9M"
    MULTI_TOKEN7="7690795267:AAEUZaj9muQaiX_EguzNP_KfaHlI-sKNxoY"
    MULTI_TOKEN8="8237961613:AAFBAJ2vHY1rV_aaamSLCx91Uw4TYI1UEw4"
    MULTI_TOKEN9="8239057772:AAGB0F4k3kNl3rjoskqVn2p0fRwZvIJTEwY"
    MULTI_TOKEN10="8405959178:AAHXvPn2Dw5NRpaV5z0aMo8lXsPuLMa8yoE"
    MULTI_TOKEN11="8284724201:AAGpnjh2Lc49vOnFy58Wp2L2R7pHv1CsOWA" 
    MULTI_TOKEN12="8481382132:AAElISWYSP-jZPFCDrix5CYxATrbbGmgJjo"
    MULTI_TOKEN13="8130057136:AAGnsTvIQYViQxEi7ZZo94UeI1H2a6ekln0"
    MULTI_TOKEN14="6732118607:AAHObBAezceVQTivgZrPx4Yo6EchsOD5S-M"

    PRE_PLAYLIST = [
        564, 563, 562, 561, 560, 559, 558, 557,
        556, 555, 554, 553, 552, 540, 539, 538, 537, 536,
        535, 534, 533, 532, 531, 530, 529, 528, 527, 526,
        525, 524, 523, 522, 521, 520, 519, 518, 517, 516,
    ]
    
    DEBUG_MODE = getenv("DEBUG_MODE", "False").lower() == "true"
    
    
    STREAM_DB_IDS = [
        DB_CHANNEL_ID,
        FAMILY_GUY_ID,
        BEN_ID,
        DORAEMON_ID,
        NARUTO_ID,
        NATIA_ID,
        PANDF_ID,
        LOCAL_ID,
    ]



    
    