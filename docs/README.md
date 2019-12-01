This README gives an overview of how to build and contribute to the documentation of Gearpump.

The documentation is included with the source of Gearpump in order to ensure that you always
have docs corresponding to your checked out version.

## Requirements

* Python >= 3.5
* [MkDocs](https://www.mkdocs.org/) >= 17.0
* [mkdocs-markdownextradata-plugin](https://github.com/rosscdh/mkdocs-markdownextradata-plugin) and [mkdocs-htmlproofer-plugin](https://github.com/manuzhang/mkdocs-htmlproofer-plugin)

Install the packages with pip

```
pip install mkdocs mkdocs-markdownextradata-plugin mkdocs-htmlproofer-plugin
```


## How to Build
Command `./build_doc.sh` can be used to create a full document folder under `site/`. 

## How to contribute

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

## How to Test

Command `mkdocs build` can be used to make a test build.

Command `mkdocs serve` can be used for debug purpose. Mkdocs will start a web server at
`localhost:8000`. Use this mode to experiment commits and check changes locally.
