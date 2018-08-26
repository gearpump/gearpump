This README gives an overview of how to build and contribute to the documentation of Gearpump.

The documentation is included with the source of Gearpump in order to ensure that you always
have docs corresponding to your checked out version.

# Requirements

You need to install ruby and ruby-dev first. On Ubuntu, you ca run command like this:

    sudo apt-get install ruby
    sudo apt-get install ruby-dev
    sudo apt-get install python-setuptools
    sudo apt-get install pip

We use Markdown to write and Jekyll to translate the documentation to static HTML. You can install
all needed software via:

    sudo pip install mkdocs
    sudo gem install html-proofer


If you are using Mac OSX 10.11+ (El Capitan), you will need to execute following command:
```
sudo gvim /Library/Ruby/Gems/2.0.0/gems/ffi-1.9.10/lib/ffi/library.rb
```
And change following code in this file:
```ruby
module FFI
...
    def self.map_library_name(lib)
        ...
        lib = Library::LIBC if lib == 'c'
        lib = Library::LIBCURL if lib == 'libcurl'
...
module Library
    CURRENT_PROCESS = FFI::CURRENT_PROCESS
    LIBC = '/usr/lib/libc.dylib'
    LIBCURL = '/usr/lib/libcurl.dylib'
```


# How to Build
Command `./build_doc.sh` can be used to create a full document folder under site/. 

# How to contribute

The documentation pages are written in
[Markdown](http://daringfireball.net/projects/markdown/syntax). 


All documents are structured with headings. From these heading, a page outline is
automatically generated for each page.

```
# Level-1 Heading  <- Used for the title of the page
## Level-2 Heading <- Start with this one
### Level-3 heading
#### Level-4 heading
##### Level-5 heading
```

Please stick to the "logical order" when using the headlines, e.g. start with level-2 headings and
use level-3 headings for subsections, etc. Don't use a different ordering, because you don't like
how a headline looks.

# How to Test

Command `mkdocs build` can be used to make a test build.

Command `mkdocs serve` can be used for debug purpose. Mkdocs will start a web server at
`localhost:8000`. Use this mode to experiment commits and check changes locally.
