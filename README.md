Title: Akashganga Renderfarm
Date: 2014-02-03 23:00
Category: projects
Tags: software
Slug: akashganga
Author: Anup
Summary: A Python and CORBA based renderfarm

#### *A Python and CORBA based renderfarm implementation released under GPL V2*

[Download Documentation Part 1](http://www.arkntek.in/extras/pdfs/akashganga/AkashGangaExplanation.pdf) Contains coursework research and observation material

[Download Documentation Part 2](http://www.arkntek.in/extras/pdfs/akashganga/AkashGangaCode.pdf) Contains source code in printer friendly formm

[Click on this line to go to the code repository on Github](https://github.com/pixelhaze/akashganga)
[Click on this line to go to the code repository on Bitbucket](http://hg.arkntek.in/akashganga)

<br/>
<br/>

During 2007-08, I undertook the Masters (PG) program at the University of Sussex, UK. The MSc in MAVE (Multimedia Applications and Virtual Environments) program introduced me to several key aspects of Multimedia technologies. During spring term, the program included the "Distributed Systems" course dealing with computing, and, data storage and retrieval over multiple computers connected to a network. Dr. Ian Wakeman, who convened the course, soon introduced the participants to CORBA and similar technologies. Under his supervision, I attempted to prototype a renderfarm completely from scratch using freely available open source tools as part of my coursework. The idea was to use any number of computers in the lab, each assuming a different role during the render process of a video. Ideally, the time required to render the video must reduce proportionally depending on the machines involved in rendering. The submitted work is being shared here for the benefit of the open source community and especially the Blender 3D community.

After a bit of research on the features of existing render farm solutions, a basic subset was arrived at. The renderfarm had to have four components:

* A render server which issues the task of rendering.
* A render client which collects details of the task and performs the rendering of designated frames.
* A job monitor which monitors the progress of the entire render process.
* A frame server which queries the clients and collected the rendered frames from the clients.

It was identified that the entire process involved file transfer at three levels.

* The application used to create a frame of render (Blender 3D in this case) had to be copied to the render client if absent.
* The 3D scene ( a .blend file ) needed to be transferred from one common location to all render clients.
* The rendered frame needed to be transferred to a single location to be combined with the remaining frames for video creation.

As, it was not discernible to identify the fastest file transfer mechanism for this application, I used different methods of file transfer at each of the levels. The first level utilised a simple shell copy which delegated the entire task as a "File copy over Network" to the host operating system (WinXP in this case). For the second level, a TFTP based implementation was experimented with. The third level involved creating a custom "chunk based file transfer" with an MD5 check.

To implement the entire distributed system, it was necessary to identify sub components at the earliest stage. CORBA was chosen as the platform which would share the program's context across the system. Python was chosen as the programming language due to ease of rapid prototyping. Several UI frontends were considered including wxPython, FLTK, Anygui, TK and GTK. GTK was chosen even though wxPython was equally applicable. PyGTK bindings to GTK proved very useful in the implementation. The "tftpy" package, provided the much needed TFTP support. One shortcoming of the "tftpy" implementation was that it supported downloads from a sever but not uploads. the "hashlib" module built-in to Python eased the required MD5 Hash support. Blender3D and FFMPEG were chosen as the two external tools, namely the 3D Software and the video utility to join frames. The ease with which Blender3D allowed specific frames to be rendered was equally complemented by FFMPEG (for Win32). For implementation purposes, MP4, MPG, FLV, WMV and AVI formats were supported by the GUI though all formats supported by FFMPEG could be easily supported.

The implementation yielded good results for a very simple Blender3D animation and proved the usability of CORBA in a renderfarm application. Though it is not industrial grade and may be greatly improved by utilising advanced CORBA features and full fledged TFTP support, it provides a glimpse of a working renderfarm. 

<br/>
<br/>

> The code has been released under [GPL V2 License](http://www.gnu.org/licenses/old-licenses/gpl-2.0.html). Please refer to licensing terms at the link provided.
> All derived works must carry credits, source code and license information provided by the previous author(s).
> Academic work must carry references to documentation and code.
