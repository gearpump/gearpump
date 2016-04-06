This README gives an overview of how to build and contribute to the documentation of Gearpump.

The documentation is included with the source of Gearpump in order to ensure that you always
have docs corresponding to your checked out version.

# Requirements

You need to install ruby and ruby-dev first. On Ubuntu, you ca run command like this:

    sudo apt-get install ruby
    sudo apt-get install ruby-dev
    sudo apt-get install python-setuptools

We use Markdown to write and Jekyll to translate the documentation to static HTML. You can install
all needed software via:

    sudo gem install jekyll
    sudo gem install kramdown
    sudo gem install html-proofer
    sudo gem install pygments.rb
    sudo easy_install Pygments

For Mac OSX you may need to do `sudo gem install -n /usr/local/bin jekyll` if you see the following error:
```
ERROR:  While executing gem ... (Errno::EPERM)
    Operation not permitted - /usr/bin/listen
```

Kramdown is needed for Markdown processing and the Python based Pygments is used for syntax
highlighting.

# How to Build
Command `./build_doc.sh` can be used to create a full document folder under site/. 

# How to contribute

The documentation pages are written in
[Markdown](http://daringfireball.net/projects/markdown/syntax). It is possible to use the
[GitHub flavored syntax](http://github.github.com/github-flavored-markdown) and intermix plain html.

In addition to Markdown, every page contains a Jekyll front matter, which specifies the title of the
page and the layout to use. The title is used as the top-level heading for the page.

    ---
    title: "Title of the Page"
    ---

Furthermore, you can access variables found in `docs/_config.yml` as follows:

    {{ site.NAME }}

This will be replaced with the value of the variable called `NAME` when generating
the docs.

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

Command `jekyll build` can be used to make a test build.

Command `jekyll serve --watch` can be used for debug purpose. Jekyll will start a web server at
`localhost:4000` and watch the docs directory for updates. Use this mode to experiment commits and check changes locally.
