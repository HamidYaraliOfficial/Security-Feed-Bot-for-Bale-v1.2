Security Feed Bot for Bale  (ربات رصد امنیتی برای بله)   v1.2
==============================================================

FAST START
  Windows     : double-click  start_windows.bat
  Linux / Mac : ./start_linux_mac.sh
  Your browser opens the control panel (http://127.0.0.1:8765). Fill the fields, press "Test", then "Start".

FEATURES
  - Full article text + ALL images, in reading order (long posts are split into several messages)
  - Automatic translation to Persian, piece by piece (rate limits are handled: slow down, wait, retry, other providers)
    Order in the channel:  header -> FULL original text (with photos) -> FULL Persian translation
  - Providers: Google (free), Lingva (free), MyMemory (free), LibreTranslate (own server), Google Cloud (API key)
  - Your own categories and your own sources (give a site URL, the RSS feed is found automatically)
  - 12 colour themes for the dashboard (pink, red, blue, ... and dark ones) - button "تم" at the top
  - Every sent post is deleted from the database right away (only a tiny dedup fingerprint stays)

Full step-by-step guide (Persian, with screenshots):  GUIDE_fa.pdf

Server mode (no panel, uses data/config.json):   python run.py --headless
Change panel port:                                python run.py --port 9000

Files
  run.py            entry point
  app/              source code (app/web = panel page + bundled Vazirmatn font, SIL OFL)
  data/config.json  your settings (contains the bot token / API keys - do not share it!)
  data/bot.db       small queue + dedup database
