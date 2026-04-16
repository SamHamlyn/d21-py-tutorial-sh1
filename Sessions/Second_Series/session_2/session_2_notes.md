#  Session 2 
 - now using py files
 - for pipelines
 
- commen to name folder with your utlity files utils

cannot have it called anything but __init__.py it tells the computer that you want to treat the files as a module
utils will need a couple things like your imports

formal ways to write the docstrings that need to be  aware of 
each function should do one thing. Make up a larger function of smaller functions

 a config files would store all the information about the 903. means that can gave 903 stuff in one place but use pipeline for other things

 enums store info liek key value pairs
 for  a function to use another written function. the called functiuon must vbe above

# Session 3

recommended black as a module to hekp with formating 


# second to last session
writing tests. very important when writing python. 
in lots of services with tools like excel and python tends to be tested with users then fixed, but python has tools to make tests 
all possible inputs.
have folder called 'tests' name py files with the word test at start and then useful info
install module called pytest
one school of thought is to write tests before you write the function. 

entere pytest into the terminal and run it, will check all files for functons starting with test_ will say if passes. 
if it fails it will say which file and line failed a test. 

in ini pythonpath = ..  the two dots say go up two directroies


final session. using sf nova which uses piodide to have things like visuals that run in the browser, translates it to web assesmbly

the app is all in 1 file rather than multiple. This is a requirement of streamlit. 

https://docs.streamlit.io/develop/api-reference

can speed up pidode with decoraters which is where you can say for a function it will save a value the first time it runs it then not run it the next time. It saves it to the browsers local memoert The danger is if it is run multiple tiems. will keep giving the first answer

