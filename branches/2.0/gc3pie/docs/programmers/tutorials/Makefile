
default:
	@echo "Type 'make ACTION', where ACTION is one of the following:"
	@echo
	@echo "clean -- Remove all temporary LaTeX files"
	@echo
	@echo "all -- Recompile all LaTeX files"
	@echo


# clean up temporary LaTeX files
clean:
	rm -f *~ *.aux *.log *.nav *.out *.rel *.snm *.toc *.vrb


# remake all LaTeX files
all: $(patsubst %.tex,%.pdf,$(wildcard *.tex))

# inputs
FIG=$(wildcard fig/*.svg)
FIG_PDF=$(FIG:.svg=.pdf)

# the default wildcard rule for generating PDFs from `.tex` files
%.pdf: %.tex gc3.sty $(FIG_PDF)
	pdflatex $< || true
	pdflatex $< || true

# wildcard rule for generating figures
fig/%.pdf: fig/%.svg
	inkscape --export-area-drawing --export-pdf=$@ $<

# additional per-file dependencies
#
# these were generated from `\includegraphics` commands in the LaTeX source with::
#
#     $ egrep --only-matching '\\includegraphics\[.+\]\{([A-Za-z0-9/._-]+)\}' *.tex \
#         | sed -r -e 's|^([a-zA-Z0-9]+)\.tex:\\includegraphics.+\{([a-zA-Z0-9/._-]+)\}|\1.pdf: \1.tex \2|'
#
