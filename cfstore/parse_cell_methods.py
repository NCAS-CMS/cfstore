import re
from ast import literal_eval


# from cfdm repo, David Hassell 

def parse_cell_methods(self, cell_methods_string, field_ncvar=None):
    """Parse a CF cell_methods string.
    .. versionadded:: (cfdm) 1.7.0
    :Parameters:
        cell_methods_string: `str`
            A CF cell methods string.
    :Returns:
        `list` of `dict`
    **Examples:**
    >>> c = parse_cell_methods('t: minimum within years '
    ...                        't: mean over ENSO years)')
    """
    if field_ncvar:
        attribute = {field_ncvar + ":cell_methods": cell_methods_string}

    incorrect_interval = (
        "Cell method interval",
        "is incorrectly formatted",
    )

    out = []

    if not cell_methods_string:
        return out

    # ------------------------------------------------------------
    # Split the cell_methods string into a list of strings ready
    # for parsing. For example:
    #
    #   'lat: mean (interval: 1 hour)'
    #
    # would be split up into:
    #
    #   ['lat:', 'mean', '(', 'interval:', '1', 'hour', ')']
    # ------------------------------------------------------------
    cell_methods = re.sub(r"\((?=[^\s])", "( ", cell_methods_string)
    cell_methods = re.sub(r"(?<=[^\s])\)", " )", cell_methods).split()

    while cell_methods:
        cm = {}

        axes = []
        while cell_methods:
            if not cell_methods[0].endswith(":"):
                break

            # TODO Check that "name" ends with colon? How? ('lat: mean
            #      (area-weighted) or lat: mean (interval: 1 degree_north comment:
            #      area-weighted)')

            axis = cell_methods.pop(0)[:-1]

            axes.append(axis)

        cm["axes"] = axes

        if not cell_methods:
            out.append(cm)
            break

        # Method
        cm["method"] = cell_methods.pop(0)

        if not cell_methods:
            out.append(cm)
            break

        # Climatological statistics, and statistics which apply to
        # portions of cells
        while cell_methods[0] in ("within", "where", "over"):
            attr = cell_methods.pop(0)
            cm[attr] = cell_methods.pop(0)
            if not cell_methods:
                break

        if not cell_methods:
            out.append(cm)
            break

        # interval and comment
        intervals = []
        if cell_methods[0].endswith("("):
            cell_methods.pop(0)

            if not (re.search(r"^(interval|comment):$", cell_methods[0])):
                cell_methods.insert(0, "comment:")

            while not re.search(r"^\)$", cell_methods[0]):
                term = cell_methods.pop(0)[:-1]

                if term == "interval":
                    interval = cell_methods.pop(0)
                    if cell_methods[0] != ")":
                        units = cell_methods.pop(0)
                    else:
                        units = None

                    try:
                        parsed_interval = literal_eval(interval)
                    except (SyntaxError, ValueError):
                        if not field_ncvar:
                            raise ValueError(incorrect_interval)

                        self._add_message(
                            field_ncvar,
                            field_ncvar,
                            message=incorrect_interval,
                        )
                        return []

                    try:
                        data = self.implementation.initialise_Data(
                            array=parsed_interval, units=units, copy=False
                        )
                    except Exception:
                        if not field_ncvar:
                            raise ValueError(incorrect_interval)

                        self._add_message(
                            field_ncvar,
                            field_ncvar,
                            message=incorrect_interval,
                            attribute=attribute,
                        )
                        return []

                    intervals.append(data)
                    continue

                if term == "comment":
                    comment = []
                    while cell_methods:
                        if cell_methods[0].endswith(")"):
                            break
                        if cell_methods[0].endswith(":"):
                            break
                        comment.append(cell_methods.pop(0))

                    cm["comment"] = " ".join(comment)

            if cell_methods[0].endswith(")"):
                cell_methods.pop(0)

        n_intervals = len(intervals)
        if n_intervals > 1 and n_intervals != len(axes):
            if not field_ncvar:
                raise ValueError(incorrect_interval)

            self._add_message(
                field_ncvar,
                field_ncvar,
                message=incorrect_interval,
                attribute=attribute,
            )
            return []

        if intervals:
            cm["interval"] = intervals

        out.append(cm)

    return out