#!/usr/bin/env python
# coding: utf-8

# # [T256075](https://phabricator.wikimedia.org/T256075) Post Deployment Data QA - Collapsible Sidebar Instrumentation
# 
# In this task we will be verifying if events fired in the test wikis are making it to the schema [DesktopWebUIActionsTracking](https://meta.wikimedia.org/wiki/Schema:DesktopWebUIActionsTracking) and if the Instrumentation is working correctly.  
# **Part 1:**
# - euwiki  
# - frwiktionary
# - ptwikiversity
# 
# **Part 2:**
# - frwiki
# - hewiki
# - fawiki

# In[1]:


import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter
import numpy as np
import pandas as pd
from tabulate import tabulate
from wmfdata import charting, hive, mariadb
from wmfdata.charting import comma_fmt, pct_fmt
from wmfdata.utils import df_to_remarkup, pct_str


# ### Count events by Wiki

# In[2]:


# Check by Wiki 

count_actions_wiki = hive.run("""
SELECT
  date_format(dt, "yyyy-MM-dd") AS date,
  event.action AS action,
  wiki AS wiki,
  COUNT(*) AS events
FROM event.desktopwebuiactionstracking
WHERE
  year = 2020 
  AND month=7 AND day > 15   
GROUP BY date_format(dt, "yyyy-MM-dd"), event.action, wiki
""")


# In[3]:


count_actions_wiki.pivot_table(index=["wiki","date"],columns=["action"])


# ### Count events by skinVersion

# In[4]:


# Check by skinVersion 

count_actions_skinVersion = hive.run("""
SELECT
  event.action AS action,
  event.skinversion as skinversion,
  COUNT(*) AS events
FROM event.desktopwebuiactionstracking
WHERE
  year = 2020 
  AND month=7 AND day > 15   
GROUP BY event.action, event.skinversion
""")


# In[5]:


count_actions_skinVersion.pivot(index='skinversion', columns='action')


# ### Count of events by skinVersion on test wikis

# In[7]:


# Check by skinVersion and wiki

count_actions_skinVersion_wiki = hive.run("""
SELECT
  event.action AS action,
  event.skinversion as skinversion,
  wiki AS wiki,
  COUNT(*) AS events
FROM event.desktopwebuiactionstracking
WHERE
  year = 2020 
  AND month=7 AND day > 15   
GROUP BY event.action, event.skinversion, wiki
""")


# In[11]:


count_actions_skinVersion_wiki.pivot_table(index=['wiki','skinversion'],columns=['action']).fillna(0)


# ### Clicks to the Collapsible Sidebar

# In[12]:


# Collapsible sidebar events by Date

count_actions_sidebar = hive.run("""
SELECT
  date_format(dt, "yyyy-MM-dd") AS date,
  event.name as name,
  COUNT(*) AS events
FROM event.desktopwebuiactionstracking
WHERE
  year = 2020 
  AND month=7 and day>15
  AND event.name LIKE 'ui.%'
GROUP BY date_format(dt, "yyyy-MM-dd"), event.name
""")


# In[13]:


count_actions_sidebar.pivot(index='date', columns='name')


# In[16]:


# Collapsible sidebar events by Wiki and date

count_actions_sidebarwiki = hive.run("""
SELECT
  date_format(dt, "yyyy-MM-dd") AS date,
  event.name as name,
  wiki AS wiki,
  COUNT(*) AS events
FROM event.desktopwebuiactionstracking
WHERE
  year = 2020 
  AND month=7 and day>18
  AND event.name LIKE 'ui.%'
GROUP BY date_format(dt, "yyyy-MM-dd"), event.name, wiki
""")


# In[17]:


count_actions_sidebarwiki.pivot_table(index=['wiki','date'], columns=['name'])


# ## Donate link events
# With the implementation of the Collapsible Sidebar, we are interested to understand in particular, what is the overall difference in interaction with the donate link?
# 
# **Donate link name = "n-sitesupport"**
# 

# In[22]:


# Donate link events

count_donate_events = hive.run("""
SELECT
  date_format(dt, "yyyy-MM-dd") AS date,
  event.name as name,
  COUNT(*) AS events
FROM event.desktopwebuiactionstracking
WHERE
  year = 2020 
  AND month=7 and day>15
  AND event.name LIKE 'n-site%'
GROUP BY date_format(dt, "yyyy-MM-dd"), event.name
""")


# In[23]:


count_donate_events


# In[29]:


# Donate link events by Wiki

count_donate_wiki = hive.run("""
SELECT
  date_format(dt, "yyyy-MM-dd") AS date,
  event.name as name,
  wiki AS wiki,
  COUNT(*) AS events
FROM event.desktopwebuiactionstracking
WHERE
  year = 2020 
  AND month=7 and day>14
  AND event.name LIKE 'n-site%'
GROUP BY date_format(dt, "yyyy-MM-dd"), event.name, wiki
""")


# No Clicks recorded on the Donate link on any of the 6 test wikis so far

# In[30]:


count_donate_wiki.pivot_table(index=['wiki','name'], columns=['date'])


# In[33]:


# Donate link events on Wikis by skinVersion

count_donate_skinVersion= hive.run("""
SELECT
  event.skinversion,
  event.name as name,
  wiki AS wiki,
  COUNT(*) AS events
FROM event.desktopwebuiactionstracking
WHERE
  year = 2020 
  AND month=7 AND day>15
  AND event.name LIKE 'n-sitesupport'
GROUP BY event.skinversion, event.name, wiki
""")


# In[39]:


count_donate_skinVersion.pivot_table(index=['name','wiki','skinversion'])


# In[41]:


# Donate link events by logged in/out users

count_donate_skinVersion_anon= hive.run("""
SELECT
  event.skinversion,
  event.name as name,
  wiki AS wiki,
  event.isanon AS anonymous_user,
  COUNT(*) AS events
FROM event.desktopwebuiactionstracking
WHERE
  year = 2020 
  AND month=7 AND day>15
  AND event.name LIKE 'n-sitesupport'
GROUP BY event.skinversion, event.name, wiki, event.isanon 
""")


# In[47]:


count_donate_skinVersion_anon.pivot_table(index=['name','wiki','skinversion'], columns=['anonymous_user']).fillna(0)


# In[48]:


# Donate link events by logged in/out users by day

count_donate_skinVersion_anon_day= hive.run("""
SELECT
  date_format(dt, "yyyy-MM-dd") AS date,
  event.skinversion,
  event.name as name,
  wiki AS wiki,
  event.isanon AS anonymous_user,
  COUNT(*) AS events
FROM event.desktopwebuiactionstracking
WHERE
  year = 2020 
  AND month=7 AND day>15
  AND event.name LIKE 'n-sitesupport'
GROUP BY date_format(dt, "yyyy-MM-dd"),event.skinversion, event.name, wiki, event.isanon 
""")


# In[49]:


count_donate_skinVersion_anon_day.pivot_table(index=
                                              ['name','wiki','date','skinversion'], 
                                              columns=['anonymous_user']).fillna(0)


# In[ ]:




