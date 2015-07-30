#!/usr/bin/python

import sys, getopt, subprocess, os, glob, re, datetime, shutil
from os import path
from os.path import basename
import stat
import time
import subprocess
from operator import itemgetter
import csv
import requests
import json
import elasticsearch
from collections import namedtuple

#run this with command line: nds_lookup_recorded_tv.py -i distilled_recorded_tv.txt
def main(argv):

    full_path = os.path.realpath(__file__)
    cur_dir = os.path.dirname(full_path)
    
    # get command line input or return usage
    input_file_name = ''
    output_file = ''
    err_output = basename(path.abspath(sys.modules['__main__'].__file__)) + ' -i <recorded_tv_directory>'

    try:
        opts, args = getopt.getopt(argv,"hi:v:o:",["help", "idir="])
    except getopt.GetoptError:
        print(err_output)
        sys.exit(2)

    if len(sys.argv) < 1:
        print(err_output)
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print(err_output)
            sys.exit()
        elif opt in ("-i", "--idir"):
            input_file_name = arg
            
    # if not all populated exit
    if input_file_name == '':
        print(err_output)
        sys.exit(2)

    #read in assignment file
    input_file = open(input_file_name)
    input_file_array = csv.reader(input_file, delimiter='|', quotechar='"')
    
    es = elasticsearch.Elasticsearch([{'host': 'qbldqatemp.tmsgf.trb', 'port': 9200}])
    
    #list of distinct shows
    show_list = [];
    
    #gather all distinct shows 
    for row in input_file_array:
        if row[0].lower() not in show_list:
            show_list.append(row[0].lower())
    
    #rest input file
    input_file.seek(0)    
    
    program_id_dictionary = {'boy meets world':'184144',
    'castle':'188601',
    'celebrity name game':'10911965',
    'cosmos':'8789932',
    'forever':'10777301',
    'futurama':'184499',
    'psych':'185254',
    'the big bang theory':'185554',
    'the fresh prince of bel-air':'184062',
    'the last man on earth':'10774951',
    'the late late show with craig ferguson':'185083',
    'the walking dead':'8282918'}
    
    available_programs_dictionary = {'Boy Meets World Twfam 2013 11 26 13 55 00-1.m4v':'The Beard',
    'Boy Meets World Twfam 2014 04 07 15 55 00-1.m4v':'Class Preunion',
    'Boy Meets World_DISN_2014_05_27_19_55_00.mp4':'The Fugitive',
    'Castle Tnt 2014 07 07 15 55 00-1.m4v':'Vampire Weekend',
    'Castle Tnt 2014 07 07 16 55 00-1.m4v':'He\'s Dead, She\'s Dead',
    'Castle Tnt 2014 07 08 15 55 00-1.m4v':'Sucker Punch',
    'Cosmos- A Spacetime Odyssey Ngc 2014 06 09 08 55 00-1.m4v':'Deeper, Deeper, Deeper Still',
    'Cosmos- A Spacetime Odyssey Ngc 2014 06 09 16 55 00-1.m4v':'The World Set Free',
    'Cosmos- A Spacetime Odyssey_NGC_2014_03_17_21_55_00.mp4':'Some of the Things That Molecules Do',
    'Forever Wten 2014 10 14 21 50 00-1.m4v':'The Pugilist Break',
    'Forever Wten 2014 10 21 21 50 00-1.m4v':'The Frustrating Thing About Psychopaths',
    'Forever Wten 2014 10 28 21 50 00-1.m4v':'New York Kids',
    'Futurama Comedy 2014 03 14 16 23 00-1.m4v':'Spanish Fry',
    'Futurama Comedy 2014 03 14 16 54 00-1.m4v':'Bend Her',
    'Futurama Comedy 2014 03 14 17 25 00-1.m4v':'Obsoletely Fabulous',
    'Psych Usa 2014 01 15 20 55 00-1.m4v':'S.E.I.Z.E. the Day',
    'Psych Usa 2014 01 20 05 55 00-1.m4v':'Cloudy With a Chance of Murder',
    'The Big Bang Theory Tbs 2014 02 12 21 25 00-1.m4v':'The Love Spell Potential',
    'The Big Bang Theory Wrgb 2013 11 21 19 55 00-1.m4v':'The Thanksgiving Decoupling',
    'The Big Bang Theory Wrgb 2013 11 28 19 55 00-1.m4v':'The Santa Simulation',
    'The Fresh Prince Of Bel-air Tbs 2013 11 18 10 55 00-1.m4v':'Get a Job',
    'The Fresh Prince Of Bel-air Tbs 2014 05 05 10 25 00-1.m4v':'Not With My Cousin You Don\'t',
    'The Fresh Prince of Bel-Air_TBS_2013_11_22_10_25_00.mp4':'I, Stank Horse',
    'The Last Man On Earth Wxxa 2015 03 01 20 55 00-1.m4v':'Alive in Tucson',
    'The Last Man On Earth Wxxa 2015 03 08 21 25 00-1.m4v':'Raisin Balls and Wedding Bells',
    'The Last Man On Earth Wxxa 2015 03 15 21 25 00-1.m4v':'Sweet Melissa',
    'The Walking Dead Amc 2013 03 25 20 00 00-1.m4v':'Days Gone Bye',
    'The Walking Dead_AMC_2013_07_04_14_36_00.mp4':'Guts'}
    
    available_programs_found_dictionary = {}
    
    #gather information from nds store in dictionary.
    season_dictionary = {}
    for show in show_list:
        url = 'http://ned-dev.tmsgf.trb:7070/NedDataServices/programs/episodesForShow?showProgramId=' + program_id_dictionary[show.lower()] + '&size=1000'
        print("show: " + show + " url: " + url)
        r = requests.get(url)
        season_dictionary[show] = r.json()

    print("")
    
    current_episode_id = 1
    
    channel_dictionary = {}
    current_channel = 1
    
    #loop over each episode and match up data from nds then construct elastic search entry and push 
    for row in input_file_array:
        show_name = row[0]
        orig_air_date = row[1]
        episode_title = row[2]
        found = False

        all_show_episodes = season_dictionary[show_name.lower()]
        for episode in all_show_episodes:
            nds_date = str(episode['originalAirDate'])
            nds_titles = episode['episodicTitles']
            
            #loop through all titles if a tile is a match consider the episode a match
            for title in nds_titles:
                nds_title_text = str(title['text'])
                
                #if the episode is a match and not yet found send to es (to avoid duplicate adds)
                if nds_title_text == episode_title and found != True:
                    print("    found - " + show_name + " - Episode Title Match - NDS: " + nds_title_text + " DVR: " + episode_title)
                    found = True
                    program_id = str(episode['programId'])
                    show_id = str(episode['parentProgramId'])
                    season_number = str(episode['seasonNumber'])
                    episode_number = str(episode['episodeNumber'])
                    available = "0"
                    
                    for description_key in available_programs_dictionary:
                        if nds_title_text == available_programs_dictionary[description_key]:
                            print("available detected")
                            available = "1"
                            available_programs_found_dictionary[description_key] = program_id
                       
                    doc = {
                        'program_id': program_id,
                        'show_id': show_id,
                        'season_number': season_number,
                        'episode_number' : episode_number,
                        'available' : available
                    }

                    res = es.index(index="episodes", doc_type='pesan', id=current_episode_id, body=doc)
                    current_episode_id = current_episode_id + 1

            #if the date exists and we didn't find the episode title try to match the dates
            if(nds_date != "None" and found != True):
                nds_year = nds_date.split('-')[0]
                nds_month = nds_date.split('-')[1]
                nds_day = nds_date.split('-')[2]
                dvr_year = orig_air_date.split('/')[2]
                dvr_month = orig_air_date.split('/')[1]
                dvr_day = orig_air_date.split('/')[0]
            
                #match up the year day and month
                if(nds_year == dvr_year and nds_month == dvr_month and nds_day == dvr_day):
                    print("    found - " + show_name + " DVR date: " + orig_air_date + " NDS date: " + episode['originalAirDate'])
                    found = True
                    program_id = str(episode['programId'])
                    show_id = str(episode['parentProgramId'])
                    season_number = str(episode['seasonNumber'])
                    episode_number = str(episode['episodeNumber'])
                       
                    doc = {
                        'program_id': program_id,
                        'show_id': show_id,
                        'season_number': season_number,
                        'episode_number' : episode_number
                    }

                    res = es.index(index="episodes", doc_type='pesan', id=current_episode_id, body=doc)
                    current_episode_id = current_episode_id + 1
        
        #mark this episode as not found.
        if(found == False):
            print("not found - " + show_name + " - " + episode_title)
    
    #reset input file
    input_file.seek(0)        
       
    #####################CREATE FAUX CHANNELS#####################
    print("")
    print("Creating channel A")
    channel_a_doc = {
        'channel_id': 1,
        'channel_name': "channel A",
        'shows': {'castle':'188601','cosmos':'8789932', 'forever':'10777301', 'the last man on earth':'10774951', 'the late late show with craig ferguson':'185083', 'the walking dead':'8282918'}
    }

    res = es.index(index="channels", doc_type='pesan', id=1, body=channel_a_doc)
    
    print("Creating channel B")
    channel_b_doc = {
        'channel_id': 2,
        'channel_name': "channel B",
        'shows': {'boy meets world':'184144','celebrity name game':'10911965', 'futurama':'184499', 'psych':'185254', 'the big bang theory':'185554', 'the fresh prince of bel-air':'184062'}
    }

    res = es.index(index="channels", doc_type='pesan', id=2, body=channel_b_doc)
    
    print("")
    print("available shows")
    for available_programs_key in available_programs_found_dictionary:
        print(available_programs_key + " " + available_programs_found_dictionary[available_programs_key])
    ##############################################################
    
    #####################CREATE FAUX RATINGS AND SHOWS#####################
    print("")
    print("Creating Show 1")
    show_1_doc = {
        'show_id': '188601',
        'show_name': 'castle',
        'rating' : '2'
    }
    res = es.index(index="shows", doc_type='pesan', id=1, body=show_1_doc)
    
    print("Creating Show 2")
    show_2_doc = {
        'show_id': '8789932',
        'show_name': 'cosmos',
        'rating' : '4'
    }
    res = es.index(index="shows", doc_type='pesan', id=2, body=show_2_doc)
    
    print("Creating Show 3")
    show_3_doc = {
        'show_id': '10777301',
        'show_name': 'forever',
        'rating' : '1'
    }
    res = es.index(index="shows", doc_type='pesan', id=3, body=show_3_doc)
    
    print("Creating Show 4")
    show_4_doc = {
        'show_id': '10774951',
        'show_name': 'the last man on earth',
        'rating' : '2'
    }
    res = es.index(index="shows", doc_type='pesan', id=4, body=show_4_doc)
    
    print("Creating Show 5")
    show_5_doc = {
        'show_id': '185083',
        'show_name': 'the late late show with craig ferguson',
        'rating' : '3'
    }
    res = es.index(index="shows", doc_type='pesan', id=5, body=show_5_doc)
    
    print("Creating Show 6")
    show_6_doc = {
        'show_id': '8282918',
        'show_name': 'the walking dead',
        'rating' : '3'
    }
    res = es.index(index="shows", doc_type='pesan', id=6, body=show_6_doc)
    
    print("Creating Show 7")
    show_7_doc = {
        'show_id': '184144',
        'show_name': 'boy meets world',
        'rating' : '2'
    }
    res = es.index(index="shows", doc_type='pesan', id=7, body=show_7_doc)
    
    print("Creating Show 8")
    show_8_doc = {
        'show_id': '10911965',
        'show_name': 'celebrity name game',
        'rating' : '2'
    }
    res = es.index(index="shows", doc_type='pesan', id=8, body=show_8_doc)
    
    print("Creating Show 9")
    show_9_doc = {
        'show_id': '184499',
        'show_name': 'futurama',
        'rating' : '4'
    }
    res = es.index(index="shows", doc_type='pesan', id=9, body=show_9_doc)
    
    print("Creating Show 10")
    show_10_doc = {
        'show_id': '185254',
        'show_name': 'psych',
        'rating' : '3'
    }
    res = es.index(index="shows", doc_type='pesan', id=10, body=show_10_doc)
    
    print("Creating Show 11")
    show_11_doc = {
        'show_id': '185554',
        'show_name': 'the big bang theory',
        'rating' : '4'
    }
    res = es.index(index="shows", doc_type='pesan', id=11, body=show_11_doc)
    
    print("Creating Show 12")
    show_12_doc = {
        'show_id': '184062',
        'show_name': 'the fresh prince of bel-air',
        'rating' : '2'
    }
    res = es.index(index="shows", doc_type='pesan', id=12, body=show_12_doc)
    
    #############################################################

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
if __name__ == "__main__":
   main(sys.argv[1:])