#! /bin/sh

repeat () {
    n="$1"
    if [ $n -gt 0 ]; then
        echo -n "$2"$(repeat $(expr $n - 1) "$2")
    fi
}

modules=$(find ../../../gc3libs ../../../gc3utils -name '*.py' \
    | cut -c10- \
    | egrep -v '/tests/' \
    | egrep -v '/compat/' \
    | sed -e 's|\.py$||' \
    | sed -e 's|/__init__||' \
    | tr '/' '.' \
    | sort)

tocitems=''
for module in $modules; do
    # reST headers must underline the whole title text
    c=$(echo -n "${module}" | wc -c)
    underline=$(repeat $(expr 2 + $c) '=')

    # create file
    filename=$(echo $module | tr '.' '/').txt
    dir=$(dirname $filename)
    # echo ">>>" mkdir -v -p $dir
    # echo ">>> ${filename}"
    mkdir -v -p $dir
    cat > $filename <<__EOF__
.. Hey Emacs, this is -*- rst -*-

   This file follows reStructuredText markup syntax; see
   http://docutils.sf.net/rst.html for more information.


\`${module}\`
${underline}
.. automodule:: ${module}
   :members:

__EOF__

    # insert generated file into TOCtree
    tocitems="$tocitems $(echo $module | tr '.' '/').txt"
done

# create ToC
cat > index.txt <<EOF
.. Hey Emacs, this is -*- rst -*-

   This file follows reStructuredText markup syntax; see
   http://docutils.sf.net/rst.html for more information.


.. _gc3libs:

-------------------------
 GC3Libs programming API
-------------------------

.. toctree::

EOF
for tocitem in $tocitems; do
    echo "   $tocitem" >> index.txt
done
