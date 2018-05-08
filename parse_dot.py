#!/usr/bin/env python

"""
parse_dot.py

parse dot file and write three outputs 
1) Edge list with properties
2) Vertex list (later, we need Vertex properties) 

Created by Tae-Hyuk (Ted) Ahn on 09/18/2014.
Copyright (c) 2013 Tae-Hyuk Ahn (ORNL). Allrights reserved.
"""


import sys, warnings, os, re
from datetime import datetime, date, time
from subprocess import Popen, PIPE, check_call, STDOUT
import getopt


###############################################################################
## Version control
###############################################################################
version = "0.4 (Alpha)"


###############################################################################
## Help message
###############################################################################
help_message = '''

  [Usage]
    parse_dot.py [options] -i <input dot file>

  [Inputs]
    -i <input filename>    : input dot format graph file

  [Options]
    -h/--help
    -v/--version

  [Outputs]
    base_filename.vertex_list.txt
    base_filename.edge_list.txt
'''


###############################################################################
## Class Usage
###############################################################################
class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg 


###############################################################################
## Exit system with error message
###############################################################################
def die(msg=None):
    if msg is not None:
        print >> sys.stderr, msg
        sys.exit(1)


###############################################################################
## string class
###############################################################################
class OptionString:

     input_filename_str = "input_filename"


###############################################################################
## Parse options
###############################################################################
def parse_option(argv, version, help_message):

    # option map dictionay
    option_map = {}

    # get arguments
    try:
        opts, args = getopt.getopt(argv[1:], "hvVi:",
                                   ["help","version"])

    # Error handling of options
    except getopt.error, msg:
        raise Usage(msg)

    # get program name
    program_name =  sys.argv[0].split("/")[-1]

    # Basic options
    for option, value in opts:
        # help
        if option in ("-h", "--help"):
            raise Usage(help_message)
        # version
        if option in ("-v", "-V", "--version"):
            print "\n%s V%s\n" % (program_name, version)
            sys.exit(0)
        # -i
        if option in ("-i"):
            option_map[OptionString.input_filename_str] = value

    # check you got two arguments
    if not OptionString.input_filename_str in option_map:
        raise Usage(help_message)

    return (option_map)


###############################################################################
## set directory path (append "/" at last if directory path doesn't have it)
###############################################################################
def set_directory_path(directory_path):
    if os.path.isdir(directory_path):
        directory_path = os.path.normpath(directory_path)
        directory_path += '/' 
    else:
        sys.stderr.write("\n** Cannot open %s.\n" % directory_path)
        die("** Program exit!")

    return directory_path


###############################################################################
## Find string between two substrings
###############################################################################
def find_between(s, first, last):
    try:
        start = s.index( first ) + len( first )
        end = s.index( last, start )
        return s[start:end]
    except ValueError:
        return ""


###############################################################################
## Work!!
###############################################################################
def work(option_map):

    # input arguments
    input_filename = option_map[OptionString.input_filename_str]
    
    # read input file
    try:
        with open(input_filename) as f:

            # get base_out filename and extension
            base_out = os.path.basename(input_filename)
            base_out_filename = base_out.split('/')[-1]
            base_out_name = '.'.join(base_out_filename.split('.')[0:-1])

            # open two outputs 
            output_vertex_list = open(base_out_name + "_vertex_list.txt", 'w')
            output_edge_list   = open(base_out_name + "_edge_list.txt", 'w')

            # initialize array
            vertex_list = []  # dot file should have unique vertex..so not necessary
                              # if this takes long time with large input, do not use array

            # read line by line
            for line in f:
                line = line.strip()

                # disregard dot structure format file such as
                # digraph G {
                # }
                dot_structure_list = ['{','}','digraph']
                if any(x in line for x in dot_structure_list):
                    continue

                # edge line should contain '->'
                elif '->' in line:
                    # split line with white space
                    line_list = line.split(" ")
                    
                    # the list should have 4 columns (1.1 -> 2.1 [label="F,33,0,0,35,2,34,35,0,32"]
                    # read1 -> read2 [label="orientation, overlap length, substitutions, edits, length1, start1, stop1, length2, start2, stop2"]
                    # orientation: F::forward, FRC::read1 overlaps with the reverse complement of read2
                    if len(line_list) < 4:
                        sys.stderr.write("** Error: Dot file edge list does not have expected length!\n")
                        die("** Program exit!")
                    else:
                        vertex_source = str(line_list[0])  # col1: source 
                        vertex_destin = str(line_list[2])  # col3: destination
                        edge_property = str(find_between(line_list[3],"\"", "\"")) # col4: edge property
                        edge_property_list = edge_property.split(",")
                        # DOT file vertex has 1.1 and 1.2 format for mates.
                        # Graphx should have integer vertex.
                        # So, convert 0.1 => 0 and 0.2 => 1, 1.1 => 2, 1.2 => 3, 2.1 => 4, 2.2 => 5, ...
                        # f(x,y) = 2x + (y-1) where x is the left part and y is the right part
                        try:
                            # calculate new vertex IDs
                            (vertex_source_x, vertex_source_y) = vertex_source.split(".")
                            (vertex_destin_x, vertex_destin_y) = vertex_destin.split(".")
                            vertex_source_new = str(2*int(vertex_source_x)+(int(vertex_source_y)-1))
                            vertex_destin_new = str(2*int(vertex_destin_x)+(int(vertex_destin_y)-1))

                            # edge properties (DOT file should provide read length)
                            orientation   = edge_property_list[0]
                            overlap_len   = edge_property_list[1]
                            substitutions = edge_property_list[2]
                            edits         = edge_property_list[3]
                            read1_length  = edge_property_list[4]
                            read1_start   = edge_property_list[5]
                            read1_stop    = edge_property_list[6]
                            read2_length  = edge_property_list[7]
                            read2_start   = edge_property_list[8]
                            read2_stop    = edge_property_list[9]

                            # F is equivalent to R and FRC is equivalent to RRC, except the start and stop positions are reversed. 
                            if orientation == "R" or orientation == "RRC":
                                read1_length, read2_length = read2_length, read1_length
                                read1_start, read2_start = read2_start, read1_start
                                read1_stop, read2_stop = read2_stop, read1_stop
                            if orientation == "R":
                                orientation = "F"
                            if orientation == "RRC":
                                orientation = "FRC"

                            # Col1: overlap orientation
                            # 0 = u<--------<v      reverse of u to reverse of v   
                            #   => This case is handled in DOT file preprocessing step and changed to 3 (u>-->v)
                            # 1 = u<-------->v      reverse of u to forward of v
                            # 2 = u>--------<v      forward of u to reverse of v
                            # 3 = u>-------->v      forward of u to forware of v
                            # Col2: overlap property F:forward, 
                            #                        FRC::read1 overlaps with the reverse complement of read2
                            # Col3~9: overlap length, substitutions, edits, start1, stop1, start2, stop2


                            # 1. check node/edge order, and change
                            # 2. check in-in and out-out
                            # For example,
                            # read 0: AGCTAAGCATTTACGATAGCCGATAGCTAAATTAC
                            # read 1:  GCTAAGCATTTACGATAGCCGATAGCTAAATTACG
                            # read 2:    TAAGCATTTACGATAGCCGATAGCTAAATTACGTT
                            #
                            # So, I expected the transitive edge reduction overlapping results as below:
                            #
                            # 0 -> 1 [label="F,34,0,0,35,1,34,35,0,33]
                            # 1 -> 2 [label="F,33,0,0,35,2,34,35,0,32]
                            #
                            # With the "mst=t" setting, but I am getting as below:
                            # digraph G { 
                            #     2 
                            #     0 
                            #     1 
                            #     1 -> 0 [label="F,34,0,0,35,0,33,35,1,34"]
                            #     1 -> 2 [label="F,33,0,0,35,2,34,35,0,32"]
                            # }
                            # change 1 -> 0 to 0 -> 1, and change the properties
                            #
                            # Method for 1: check starts of reads. if read1_start < read2_start, then swap
                            # Method for 2: (read1_start) - ((read2_len - 1) - (read2_start) > 0, then >----< (out - out)
                            # Method for 2: (read1_start) - ((read2_len - 1) - (read2_start) < 0, then <----> (in - in)

                            # overlap forward to forward(orientation num: 3)
                            if orientation == "F":
                                # Method for 1
                                if int(read1_start) < int(read2_start): # print swap edge list and properties
                                    output_edge_list.write(vertex_destin_new + "\t" + vertex_source_new + "\t" +
                                                           "3," + orientation + "," +
                                                           overlap_len + "," +
                                                           substitutions + "," +
                                                           edits + "," +
                                                           read2_start + "," +
                                                           read2_stop + "," +
                                                           read1_start + "," +
                                                           read1_stop + "\n")
                                else: # print same order
                                    output_edge_list.write(vertex_source_new + "\t" + vertex_destin_new + "\t" +
                                                           "3," + orientation + "," +
                                                           overlap_len + "," +
                                                           substitutions + "," +
                                                           edits + "," +
                                                           read1_start + "," +
                                                           read1_stop + "," +
                                                           read2_start + "," +
                                                           read2_stop + "\n")
            
                            # overlap reverse (FRC)
                            elif orientation == "FRC":
                                if int(read1_start) - ((int(read2_length) - 1) - int(read2_start)) > 0: # 2 = u>-----<v
                                    output_edge_list.write(vertex_source_new + "\t" + vertex_destin_new + "\t" +
                                                           "2," + orientation + "," +
                                                           overlap_len + "," +
                                                           substitutions + "," +
                                                           edits + "," +
                                                           read1_start + "," +
                                                           read1_stop + "," +
                                                           read2_start + "," +
                                                           read2_stop + "\n")
                                else:   #1 = u<-------->v      reverse of u to forward of v
                                    output_edge_list.write(vertex_source_new + "\t" + vertex_destin_new + "\t" +
                                                           "1," + orientation + "," +
                                                           overlap_len + "," +
                                                           substitutions + "," +
                                                           edits + "," +
                                                           read1_start + "," +
                                                           read1_stop + "," +
                                                           read2_start + "," +
                                                           read2_stop + "\n")

                            # else, then error
                            else:
                                sys.stderr.write("** Error: line =%s\n" %(line))
                                sys.stderr.write("** Error: Check DOT file orientation on edges!\n")
                                die("** Program exit!")

                        except:
                            sys.stderr.write("** Error: line =%s\n" %(line))
                            sys.stderr.write("** Error: Check DOT file vertices on edges!\n")
                            die("** Program exit!")

                # vertex line
                else:
                    vertex = line
                    try:
                        (vertex_x, vertex_y) = vertex.split(".")
                        vertex_new = str(2*int(vertex_x)+(int(vertex_y)-1))
                        vertex_list.append(vertex_new)
                    except:
                        sys.stderr.write("** Error: Check DOT file vertices!\n")
                        die("** Program exit!")

            # sort vertex list
            # if this takes long time with large input, do not sort
            vertex_list = sorted(set(vertex_list))
            for vertex in vertex_list:
                output_vertex_list.write(str(vertex)+"\n")
    
                
    except IOError as e:
        sys.stderr.write("** Error: Cannot open %s\n" %(input_filename))
        die("** Program exit!")


        

###############################################################################
def main(argv=None):
###############################################################################

    # try to get arguments and error handling
    try:
        if argv is None:
            argv = sys.argv
        try:
            # get program name
            program_name =  os.path.basename(sys.argv[0])

            # parse option
            (option_map) = parse_option(argv, version, help_message)

            # display work start and time record
            start_time = datetime.now()
            sys.stdout.write("\n*********************************************************\n")
            sys.stdout.write("* Beginning %s run (V%s)\n" % ( program_name, version))

            work(option_map)

            # time record, calculate elapsed time, and display work end
            finish_time = datetime.now()
            duration = finish_time - start_time
            sys.stdout.write("* Ending %s run\n" % (program_name))
            sys.stdout.write("* Total Elapsed Time =  %s [seconds]\n" % (duration))
            sys.stdout.write("*********************************************************\n\n")


        # Error handling
        except Usage, err:
            sys.stderr.write("%s: %s\n" %(os.path.basename(sys.argv[0]), str(err.msg)))
            return 2


    # Error handling
    except Usage, err:
        sys.stderr.write("%s: %s\n" %(os.path.basename(sys.argv[0]), str(err.msg)))
        sys.stderr.write("for help use -h/--help")
        return 2


###############################################################################
## If this program runs as standalone, then exit.
###############################################################################
if __name__ == "__main__":
    sys.exit(main())
