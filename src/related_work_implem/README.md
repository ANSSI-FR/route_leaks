# Detecting Large Route Leaks implementation

This is our expanded implementation of the algorithm presented by researchers of university of Arizona.

https://pdfs.semanticscholar.org/b8eb/a71bba98da54c3313f1c9cbc79b8581c0bd5.pdf

## Algorithm

### Step A + B: Get origin changes
Keep tracks of all ASes that have announced each prefix.

### Step C: Legitimate Announcements
Reduce detection noise by identifying origin changes that can be legitimate.

#### stable set
An AS that have announced a prefix during more than one day is considered legitimate for the announce

#### related set - sub prefixes
If AS X is in the stable of of prefix p, X is in the related set of any sub-prefix of p.

#### related set - network connectivity
If AS a0 originates prefix p through AS path {ak ... a1 a0} for more than one day, 
it can be inferred that a1 is the upstream of a0.
Then if a0 is in the stable set of p, a1 is in its related set.

#### related set - IXP
AS participating in an Internet Exchange Point indirectly owns the prefix associated with the exchange points.

#### related set - whois contact
AS belongs to related set if it is in the same organization as its victim. 
This could be inferred from ASes contact email domains.

### Step D: Detect Origin Conflicts
Attackers are ASes that have announced a prefix while neither in the stable or related set. 
Victims are ASes in the stable set, the number of victims is the number of attacked stable sets,
this number is called the offense value.

### Step E: Identify Large Route Leaks Events
A route leak is an attack with a offense value bigger than a threshold, set to 10.


## Adapted Algorithm

### Get filtered conflicts

    get_filtered_conflicts(*conflicts_from_tabi*)

Retrieve all conflicts (from Tabi "conflicts" data), filtering the following cases:  
- VALID: the conflicted announce is validated by either a route object or a ROA  
    -> This is an addition compared to the original algorithm  
- RELATION: a relation has been inferred between ASes using WHOIS information    
    -> This is a filter including "whois contact" from original algorithm    
- DIRECT  
    -> This corresponds to "network connectivity"  
- NODIRECT: ASes have been seen in the same AS path, showing they are related is some way  
    -> This is going one step further compare to "network connectivity"  

Hence this correspond to steps A, B, partial C and D.  


### Find stable sets

    get_stable_sets(*prefixes_from_tabi*)

This step is meant to prepare the next one, it does not match a step in the original algorithm.  
It calculates all "stable sets" using Tabi "prefixes" data. 
For every prefix announced, it count the number of days each AS has announced it.  

### More filters on conflicts

    get_conflicts(*filtered_conflicts*, *stable_sets*)
    
Applies stable sets and ixp filters on conflicts from get_filtered_conflicts.  
This fulfills step C from original algorithm.


### Identify LRL

    get_lrl(*conflicts*, *threshold*)
    
Exactly step E of original algorithm. 
It calculate the offense value for each conflicting AS 
and keep those with offense value bigger that the threshold as route leaks.