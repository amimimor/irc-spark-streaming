#!/usr/bin/python
class IRCMethods:   
    @classmethod
    def get_src(line):
       src = lines[2].ltrim("#")
       return src
    
    @classmethod
    def parse_line(s):
       lines = s.split(" ")
       src = self.get_src(lines)
       return lines


import unittest

class MainTester(unittest.TestCase):
   input_example = """:rc-pmtpa!~rc-pmtpa@special.user PRIVMSG #en.wikipedia :14[[07Edward Venables-Vernon-Harcourt14]]4 M10 02https://en.wikipedia.org/w/index.php?diff=699135055&oldid=671587254 5* 03Bender235 5* (+0) 10clean up; http->https (see [[WP:VPR/Archive 127#RfC: Should we convert existing Google and Internet Archive links to HTTPS?|this RfC]]) using [[Project:AWB|AWB]]"""
   lines = input_example.split(" ")

   def parse_test(self):
       m = IRCMethods.get_src(line)
       self.assertEqual("#en.wikipedia", m)


if __name__ == '__main__':
    unittest.main()
