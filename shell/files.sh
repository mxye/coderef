#type q to exit from manual, man or aws help

#count the number of files in current directory
ls -1 | wc -l

#count the number of files with string "str" in the name, in current directory
#may get argument list too long error
ls -d *str* | wc -l

#if so, use this in the current directory:
find . -type f -name 'str'  | wc -l
find . -not -type d | egrep -v 'str' | wc -l

#show first few lines of file

head -3 file_name*.*

head -3 ~/Projects/projectdata/ppl/resbigram.json

head -200 *resume_148955*.*

#copy single file 
rsync --ignore-existing ~/Documents/pdfresumes/resume_244001.* ~/Projects/projectdata/ppltemp/

rsync --ignore-existing ~/Projects/projectdata/ppltxt1/resume_148955*.* ~/Projects/projectdata/ppltemp/

#search for string in file. list full line and the files that contain the string
grep -r 'pattern_to_match' directory_to_search
#if just want line number:
grep -n 'pattern_to_match' directory/file_Name



#rsync --ignore-existing ~/Documents/pdfresumes/resume_182984.* ~/Projects/projectdata/ppltemp/



resume_124244: career builder
resume_124608: same
resume_1786: some kind of top sales website


get txt files
