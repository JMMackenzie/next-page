# Efficient Query Processing Techniques for Next-Page Retrieval

This is the code and data repository for the paper **Efficient Query Processing Techniques for Next-Page Retrieval** by Joel Mackenzie, Matthias Petri, and Alistair Moffat. 

## Reference
If you use our efficient next-page extensions to PISA in your own research, you can cite the following reference:
```
@article{mpm21-irj,
  author = {J. Mackenzie and M. Petri and A. Moffat},
  title = {Efficient Query Processing Techniques for Next-Page Retrieval},
  journal = "Information Retrieval",
  year = {2022},
  volume = {},
  number = {},
  pages = {},
  note = {To appear}
}
```

## Changes
This is a modified version of the PISA engine which incorporates the experiments from the "next page retrieval" paper
to assist in reproducibility. 

## Algorithms implemented
Each of the following algorithms is implemented in terms of the `wand` or `block_max_wand` index traversal algorithms. 
The particular next-page algorithms are as follows:

- `*_method_1` : This query type implements Method 1 from the paper; it retains **ejected documents** to build the second page.

- `*_method_2` : This query type implements Method 2 from the paper; it retains ejected documents as well as **near miss** documents.

- `*_method_3` :

So, if you wanted to use `block_max_wand` with Method 2, you'd specify `block_max_wand_method_2` as the algorithm.

## Annotations
To make life (an epsilon) easier, the modified aspects of the original PISA code have been annotated
with an `//NEXTPAGE` comment. Hopefully this makes the modifications easier to track for anyone
interested on extending or improving our work.

# PISA@a154529

This repo is based on [PISA](https://github.com/pisa-engine/pisa/) at commit `a154529` -- please see the original
repo if you are interested in more details.


## Reference

If you use PISA in your own research, you can cite the following reference:
```
@inproceedings{MSMS2019,
  author    = {Antonio Mallia and Michal Siedlaczek and Joel Mackenzie and Torsten Suel},
  title     = {{PISA:} Performant Indexes and Search for Academia},
  booktitle = {Proceedings of the Open-Source {IR} Replicability Challenge co-located
               with 42nd International {ACM} {SIGIR} Conference on Research and Development
               in Information Retrieval, OSIRRC@SIGIR 2019, Paris, France, July 25,
               2019.},
  pages     = {50--56},
  year      = {2019},
  url       = {http://ceur-ws.org/Vol-2409/docker08.pdf}
}
```
