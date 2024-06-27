"""
This module contains the union find algorithm that postprocessing.py uses.

Author: Molly Rossow
Date: June 2016
Last modified January 2023 by Sabrina Sedovic to function for record linkage
"""
import csv

class UnionFind():
    """Union Find implementation to find super groups for the multi-systems
       families project.

    Attributes:
        group_to_item_set (dict): Maps group name to set of items in group.
        item_to_group (dict): Maps item to group name.
        group_to_top_group (dict): Maps the name of group that has been replaced
            in a merge to the name of the group it was merged with.

    """

    def __init__(self):
        self.group_to_item_set = {}
        self.item_to_group = {}
        self.group_to_top_group = {}
        self.top_group_to_merged_groups = {}
        self.count = 1

    def find(self, item):
        """Returns the name of the group that contains item.

        Args:
           item (hashable): Item in a group.

        Returns:
            hashable: Name of group.

        """
        return self.item_to_group(item)

    def union(self, group_a, group_b):
        """Merges group a and group b.

        Args:
            group_a (hashable): Name of group to merge. This groups name will be
                kept.
           group_b (hashable): Name of group to merge. This groups name will be
                added to group_to_top_group.

        """
        if group_a == group_b:
            return

        group_to_top_group = self.group_to_top_group
        group_to_item_set = self.group_to_item_set
        item_to_group = self.item_to_group
        top_group_to_merged_groups = self.top_group_to_merged_groups

        # Check if these two groups have been merged before.
        if group_a in group_to_top_group:
            if group_b == group_to_top_group[group_a]:
                return
        if group_b in group_to_top_group:
            if group_a == group_to_top_group[group_b]:
                return

        # Merge sets. Merge group_b set into group a.
        # Find top groups.
        if group_a in group_to_top_group:
            top_a = group_to_top_group[group_a]
        else:
            top_a = group_a
        if group_b in group_to_top_group:
            top_b = group_to_top_group[group_b]
        else:
            top_b = group_b

        group_b_set = group_to_item_set[top_b]
        group_to_item_set[top_a].update(group_b_set)
        del group_to_item_set[top_b]

        # Update top groups.
        group_to_top_group[group_b] = top_a  # group_a becomes the top group.
        group_to_top_group[top_b] = top_a

        # Assign group_b to top group a.
        if top_a in top_group_to_merged_groups:
            top_group_to_merged_groups[top_a].add(group_b)
            top_group_to_merged_groups[top_a].add(top_b)
        else:
            top_group_to_merged_groups[top_a] = {group_b}
            top_group_to_merged_groups[top_a].add(top_b)

        # Assign groups previously assigned to group_b to group_a
        if top_b in top_group_to_merged_groups:
            top_group_to_merged_groups[top_a].update(top_group_to_merged_groups[top_b])

        # Groups that previously had group_b as their top group now have
        # group_a.
        if top_b in top_group_to_merged_groups:
            for g in top_group_to_merged_groups[top_b]:
                group_to_top_group[g] = top_a
            del top_group_to_merged_groups[top_b]

        # Update item to group
        for i in group_b_set:
            item_to_group[i] = top_a


    def add_item_dedup(self, group, item):
        """Add an item to a group. If the item is already in a group, merge the
       two groups.

        Args:
            group (hashable): Group name.
            item (hashable): Item to be added to group.

        """

        item_to_group = self.item_to_group
        group_to_item_set = self.group_to_item_set
        group_to_top_group = self.group_to_top_group

        if item in item_to_group: # Item has already been added to a group.
            # If the item is already part of the current group, do nothing.
            if group == item_to_group[item]:
                return
                self.count += 1
            # If the item is in a different group, merge those groups:
            elif group in group_to_item_set: # Group exists.
                self.union(item_to_group[item], group)
            elif group in group_to_top_group: # If group has already been
                                              # merged, merge top group instead.
                if group_to_top_group[group] != item_to_group[item]:
                    self.union(group_to_top_group[group], item_to_group[item])
            else: # Group needs to be created then merged.
                group_to_item_set[group] = {item}
                self.union(item_to_group[item], group) # Is this line needed????
        else: # Item has not been added to any group.
            # If the group has already been merged, add to the merged group.
            if group in group_to_top_group:
                # Add item to top group.
                top_group = group_to_top_group[group]
                group_to_item_set[top_group].add(item)
                item_to_group[item] = top_group
            elif group in group_to_item_set: # If the group exists, add it to
                                             # that group.
                group_to_item_set[group].add(item)
                item_to_group[item] = group
            else: # If the group doesn't exist yet, create the group.
                group_to_item_set[group] = {item}
                item_to_group[item] = group
        self.count += 1

    def add_item_M2M(self, group, item, passnum):
        """Add an item of two values to a group.
            If either value is already in a group, merge the
            two groups.

        Args:
            group (hashable): Group name.
            item (hashable): Tuple of values to be added to a group
            passnum (int): the pass the item was found in

        """
        a, b = item
        item = (a, b, passnum)
        item_to_group = self.item_to_group
        group_to_item_set = self.group_to_item_set
        group_to_top_group = self.group_to_top_group

        group_to_item_set[group] = {item}
        item_to_group[item] = group
        # first value has been seen
        if a in item_to_group:
            grp = item_to_group[a]
            # The second value has not been seen, add value and item to group
            if b not in item_to_group:
                self.union(grp, group)
                item_to_group[b] = grp
                group_to_item_set[grp].add(item)
                group_to_item_set[grp].add(b)
            # The two items are in different groups, combine groups
            elif item_to_group[a] != item_to_group[b]:
                grpb = item_to_group[b]
                self.union(grp, group)
                self.union(grp, grpb)
                item_to_group[b] = item_to_group[a]
                group_to_item_set[item_to_group[a]].add(item)
            # the two items are already in the same group, add item to group
            elif item_to_group[a] == item_to_group[b]:
                self.union(grp, group)
                group_to_item_set[grp].add(item)

        # second item has been seen (can assume first item was not seen then)
        elif b in item_to_group:
            grpb = item_to_group[b]
            self.union(grpb, group)
            item_to_group[a] = grpb
            group_to_item_set[grpb].add(item)
            group_to_item_set[grpb].add(a)

        # neither item has been seen yet, add both to group
        else:
            group_to_top_group[group] = group
            item_to_group[a] = group
            item_to_group[b] = group
            group_to_item_set[group].add(a)
            group_to_item_set[group].add(b)
        self.count += 1


    def delete_super_case(self, group):

        # Delete from item to group.
        largest_group = self.group_to_item_set[group]
        for i in largest_group:
            del self.item_to_group[i]

        # Delete from group_to_top_group
        for j in self.top_group_to_merged_groups[group]:
            del self.group_to_top_group[j]

        # Delete from group_to_item_set
        del self.group_to_item_set[group]

        # Delete from top_group_to_merged_group
        del self.top_group_to_merged_groups[group]

    def add_csv(self, file_name, matchtype, strictness=None, rowid=0):
        """Adds the contents of a CSV file to the UnionFind.

        Skips the first row. File should have this format:

        id_a_0, id_b_0
        ...
        id_a_x, id_b_y

        Args:
            file_name (str): Full path of the file.
            matchtype (str): dedup or M2M. If dedup, ids
                are from same set of values. If M2M, ids
                are from two different files, and the originally
                matching should be kept

        Returns:
            rowid (int) the last row number added. UnionFind object updated
            in place
        """
        # create a new id to combine groups by
        with open(file_name) as f:
            csvreader = csv.DictReader(f)
            for row in csvreader:
                if strictness is None or row[strictness] is None \
                  or row[strictness].upper() != "FALSE":
                    if matchtype == "dedup":
                        self.add_item_dedup(rowid, row["indv_id_a"].strip())
                        self.add_item_dedup(rowid, row["indv_id_b"].strip())
                    else:
                        self.add_item_M2M(rowid, ("a_" + row["indv_id_a"].strip(),
                                                  "b_" + row["indv_id_b"].strip()),
                                          row["passnum"])
                    # increase row number
                    rowid += 1
        return rowid

    def get_super_case_sizes(self):
        """Returns a list of super case sizes"""
        return sorted([len(v) for v in self.group_to_item_set.values()])

    def get_super_cases(self):
        return self.group_to_item_set

    def print_uf(self):
        """Prints groups and the items in each group."""

        for group, items in self.group_to_item_set.items():
            print ("{0} : {1}".format(group, items))

    def save_uf_as_csv(self, file_name, columns = None):
        """Saves super cases to a csv file.
        """
        group_to_item_set = self.get_super_cases()

        with open(file_name, 'w') as f:
            csvwriter = csv.writer(f)
            if columns:
                csvwriter.writerow(columns)
            else:
                csvwriter.writerow(['group', 'item'])
            sc_id = 1
            for sc in group_to_item_set:
                for id in group_to_item_set[sc]:
                    csvwriter.writerow([sc_id, id])
                sc_id += 1

    def load_uf_csv(self, file_name):
        """Loads a csv file with supercase, item columns.
        """
        group_to_item_set = self.get_super_cases()
        with open(file_name, 'r') as f:
            csvreader = csv.reader(f)
            next(csvreader)
            for row in csvreader:
                group = row[0]
                item = row[1]
                if group in group_to_item_set:
                    group_to_item_set[group].add(item)
                else:
                    group_to_item_set[group] = {item}
