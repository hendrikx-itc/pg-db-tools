# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SPHINXPROJ    = MyProjectName
SOURCEDIR     = source
BUILDDIR      = build
TARGETS       = html latexpdf
SCHEMA_YAML   = {{ schema_yaml_file }}

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
$(TARGETS): Makefile $(SOURCEDIR)/schema/reference.rst $(SOURCEDIR)/schema/diagram.dot
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

# pg-db-tools specific build targets
$(SOURCEDIR)/schema/reference.rst: $(SCHEMA_YAML)
        db-schema compile rst --out-encoding=utf-8 $< -o $@

$(SOURCEDIR)/schema/diagram.dot: $(SCHEMA_YAML)
        db-schema compile dot --href-prefix="../schema/reference.html#table-" --out-encoding=utf-8 $< -o $@
