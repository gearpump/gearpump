#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'fileutils'
include FileUtils

if (ENV['BUILD_API'] == '1')
    # Build Scaladoc for Java/Scala
    puts "Moving to project root and building API docs."
    curr_dir = pwd
    cd("..")

    puts "Running 'sbt clean unidoc' from " + pwd + "; this may take a few minutes..."
    # system("sbt", "clean", "unidoc")

    puts "Moving back into docs dir."
    cd(curr_dir)

    puts "Removing old docs"
    puts `rm -rf _site/api`

    # Copy over the unified ScalaDoc for all projects to api/scala.
    # This directory will be copied over to _site when `jekyll` command is run.
    source = "../target/scala-" + ENV['SCALA_VERSION'] + "/unidoc"
    dest = "_site/api/scala"

    puts "Making directory " + dest
    mkdir_p dest

    # From the rubydoc: cp_r('src', 'dest') makes src/dest, but this doesn't.
    puts "cp -r " + source + "/. " + dest
    cp_r(source + "/.", dest)

    # Append custom JavaScript
    js = File.readlines("./js/api-docs.js")
    js_file = dest + "/lib/template.js"
    File.open(js_file, 'a') { |f| f.write("\n" + js.join()) }

    # Append custom CSS
    css = File.readlines("./css/api-docs.css")
    css_file = dest + "/lib/template.css"
    File.open(css_file, 'a') { |f| f.write("\n" + css.join()) }
end
