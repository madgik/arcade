import pandas
MARKER = b'PAR1'
import struct
import msgpack
from io import BytesIO as cStringIO
from array import array
import zlib

def write_mostlyordered_block(f,coldict,chunk,ll):
    output = cStringIO()
    output.truncate(0)
    headindex = [0]*4
    minmax = [None]*2
    minmax[0] = min(chunk)
    minmax[1] = max(chunk)
    type = 'i'*len(headindex)
    output.write(struct.pack(type, *headindex))
    l1 = output.tell()
    output.write(msgpack.dumps(minmax))
    l2 = output.tell()
    headindex[0] = l2-l1
    if ll<256:
        type1 = 'B'
        headindex[2] = 2
    elif ll<65536:
        type1 = 'H'
        #type1 = np.int16
        headindex[2] = 1
    else:
        	type1 = 'i'
        #type1 = np.int32
    l3 = output.tell()
    #offsets = [coldict[y] for y in chunk]
    #output.write(struct.pack(type1*len(offsets), *offsets))
    if minmax[0] != minmax[1]:
        offsets = [coldict[y] for y in chunk]
        output.write(array(type1,offsets).tostring())
    else:
    	output.write(struct.pack('i',coldict[minmax[0]]))
    l4 = output.tell()
    headindex[1] = l4-l3
    headindex[3] = len(chunk)
    output.seek(0)
    output.write(struct.pack(type, *headindex))
    f.write(output.getvalue())

def mostlyordered(data,filen, row_group_offsets, pitch, lookahead, batch):
    f = open(filen,"w+b")
    f.write(MARKER)
    headindex = [0]*6
    l = len(data)
    headindex[0] = l
    headindex[1] = len(data.columns)
    type = 'i'*len(headindex)
    f.write(struct.pack(type, *headindex))
    nparts = max((l - 1) // row_group_offsets + 1, 1)
    chunksize = max(min((l - 1) // nparts + 1, l), 1)
    batch_size = 20
    parts = len(data)//batch_size
    batch = [None]*batch_size

    for col in data:
        estimatedvals = int(len(data[col][0:int(lookahead*l)].unique())/lookahead)
        global_dict = {}
        glob_dict = {}
        #global_list = [None]*estimatedvals*pitch
        global_list = []
        unsorted_list = []
        i = 0
        j = 1
        k=0
        import bisect 

        countvals = 0
        unsorted_counter = estimatedvals*pitch
        for rowid, val in enumerate(data[col]):
            countvals+=1
            if val not in glob_dict:
                batch[k] = val
                glob_dict[val] = 1
                k+=1
                if k == 20:
                    for val in batch:
                        temppos = 0
                        if len(global_list) == 0: 
                            global_list.append((val,((estimatedvals*pitch-1))//2))
                            global_dict[val]=(estimatedvals*pitch-1)//2
                        else:
                            for ll,v1 in enumerate(global_list):
                                
                                if v1[0] > val:
                                    if v1[1]-temppos>1:
                                        bisect.insort(global_list, (val,(v1[1]+temppos)//2))
                                        global_dict[val] = (v1[1]+temppos)//2
                                        break
                                    else:
                                        unsorted_list.append((val,unsorted_counter))
                                        global_dict[val] = unsorted_counter
                                        unsorted_counter += 1
                                        break

                                elif  ll == len(global_list)-1:
                                    if estimatedvals*pitch-temppos>1:
                                        global_list.append((val,((estimatedvals*pitch-1)+temppos)//2))
                                        global_dict[val] = ((estimatedvals*pitch-1)+temppos)//2
                                        break
                                    else:
                                        unsorted_list.append((val,unsorted_counter))
                                        global_dict[val] = unsorted_counter
                                        unsorted_counter += 1
                                        break
                                else:
                                    temppos = v1[1]
                    k = 0
                    
            if (rowid >= chunksize and rowid%chunksize == 0) or rowid == l-1:
              
              if val not in glob_dict:
                  batch[k]=val
                  k+=1
                  glob_dict[val] = 1
              for val in batch[0:k]:
                        temppos = 0
                        if len(global_list) == 0: 
                            global_list.append((val,((estimatedvals*pitch-1))//2))
                            global_dict[val]=(estimatedvals*pitch-1)//2
                        else:
                            for ll,v1 in enumerate(global_list):
                                
                                if v1[0] > val:
                                    if v1[1]-temppos>1:
                                        bisect.insort(global_list, (val,(v1[1]+temppos)//2))
                                        global_dict[val] = (v1[1]+temppos)//2
                                        break
                                    else:
                                        unsorted_list.append((val,unsorted_counter))
                                        global_dict[val] = unsorted_counter
                                        unsorted_counter += 1
                                        break

                                elif  ll == len(global_list)-1:
                                    if estimatedvals*pitch-temppos>1:
                                        global_list.append((val,((estimatedvals*pitch-1)+temppos)//2))
                                        global_dict[val] = ((estimatedvals*pitch-1)+temppos)//2
                                        break
                                    else:
                                        unsorted_list.append((val,unsorted_counter))
                                        global_dict[val] = unsorted_counter
                                        unsorted_counter += 1
                                        break
                                else:
                                    temppos = v1[1]
              if rowid == l-1:
                write_mostlyordered_block(f, global_dict, data[col][l-countvals-1: l], unsorted_counter)        
        
              else:
                
                write_mostlyordered_block(f, global_dict, data[col][(rowid//chunksize)*chunksize-chunksize: (rowid//chunksize)*chunksize], unsorted_counter)  
              k=0 
              countvals=0     
        
            
        finallist = [""]*(estimatedvals*pitch + unsorted_counter)
       
       
        for k,val in global_dict.items():
            finallist[val] = k
        l1 = f.tell()
        f.write(msgpack.dumps(finallist))
        l2 = f.tell()
        headindex[2] = l1
        headindex[3] = l2-l1
        headindex[4] = estimatedvals*pitch
        headindex[5] = unsorted_counter
        f.seek(4)
        f.write(struct.pack(type, *headindex))
        
        f.close()




def globalorder(f,global_dict,chunk):
    output = cStringIO()
    output.truncate(0)
    headindex = [0]*4
    minmax = [None]*2
    minmax[0] = min(chunk)
    minmax[1] = max(chunk)
    type = 'i'*len(headindex)
    output.write(struct.pack(type, *headindex))
    l1 = output.tell()
    output.write(msgpack.dumps(minmax))
    l2 = output.tell()
    headindex[0] = l2-l1

    coldict = dict(((x, y) for y, x in enumerate(global_dict)))
    l3 = output.tell()
    if minmax[0] != minmax[1]:
        offsets = [coldict[y] for y in chunk]
        ll = max(offsets)
        if ll<256:
            type1 = 'B'
            headindex[2] = 2
        elif ll<65536:
            type1 = 'H'
        #type1 = np.int16
            headindex[2] = 1
        else:
            type1 = 'i'
        #type1 = np.int32
    
    
        
        output.write(struct.pack(type1*len(offsets), *offsets))
    else:
    	output.write(struct.pack('i',coldict[minmax[0]]))
    #output.write(array(type1,[coldict[y] for y in chunk]).tostring())
    l4 = output.tell()
    headindex[1] = l4-l3
    headindex[3] = len(chunk)
    output.seek(0)
    output.write(struct.pack(type, *headindex))
    f.write(output.getvalue())


def write_global(data,filen, ordering, row_group_offsets):
    f = open(filen,"w+b")
    f.write(MARKER)
    headindex = [0]*3
    l = len(data)
    headindex[0] = l
    headindex[1] = len(data.columns)
    type = 'i'*len(headindex)
    f.write(struct.pack(type, *headindex))
    nparts = max((l - 1) // row_group_offsets + 1, 1)
    chunksize = max(min((l - 1) // nparts + 1, l), 1)
    
    
    for col in data:
        if ordering == 1:
            global_dict = sorted(data[col].unique())
        else:
            global_dict = list(data[col].unique())
        l1 = f.tell()
        f.write(msgpack.dumps(global_dict))
        l2 = f.tell()
        headindex[2] = l2-l1
        f.seek(4)
        f.write(struct.pack(type, *headindex))
        f.seek(l2)
        i=0 
        j=1
        for part in range(nparts):
            chunk = data[col][chunksize*i:chunksize*j]
            globalorder(f,global_dict,chunk)
            i+=1
            j+=1
            



import numpy as np

def globalindirect(f,global_dict,ordering,chunk):
    output = cStringIO()
    indirect = 0
    output.truncate(0)
    headindex = [0]*7
    minmax = [None]*2
    minmax[0] = min(chunk)
    minmax[1] = max(chunk)
    local_dict = np.sort(chunk.unique())
    
    type = 'i'*len(headindex)
    output.write(struct.pack(type, *headindex))
    l1 = output.tell()
    output.write(msgpack.dumps(minmax))
    l2 = output.tell()
    headindex[0] = l2-l1
    coldict = {}
    
    size_1 = len(local_dict) 
    size_2 = len(global_dict) 
  
    res2 = [None]*size_1
    i, j = 0, 0
    if not ordering:
      res2 = list(np.intersect1d(local_dict,global_dict,assume_unique=True,return_indices=True)[2])
    else:
      while i < size_1: 
        if local_dict[i] > global_dict[j]: 
            j+=1
        elif local_dict[i] == global_dict[j]:
          res2[i] = j
          i += 1
          j += 1
    
    
    ### chose if indirect
    locallen = len(local_dict)
    maxres = max(res2)
    globallen = len(global_dict)
    datalen = len(chunk)
    if locallen < 256:
            s1 = 1
    elif locallen < 65536:
            s1 = 2
    else:
            s1 = 4
    if globallen < 256:
            s2 = 1
    elif globallen < 65536:
            s2 = 2
    else:
            s2 = 4
    if maxres < 256:
            s3 = 1
    elif maxres < 65536:
            s3 = 2
    else:
            s3 = 4
            
    if datalen*s1 + locallen*s3 < datalen*s2:
        indirect = 1
    #######################
 
    if indirect:  
        coldict = dict(((x, y) for y, x in enumerate(local_dict)))
    else:
        coldict = dict(((x, y) for y, x in enumerate(global_dict)))
        
        
    l3 = output.tell()
    if minmax[0] != minmax[1]:
        offsets = [coldict[y] for y in chunk]
        ll = max(offsets)
        if ll<256:
            type1 = 'B'
            headindex[2] = 2
        elif ll<65536:
            type1 = 'H'
        #type1 = np.int16
            headindex[2] = 1
        else:
            type1 = 'i'
        #type1 = np.int32
        globall = max(res2)
        if globall<256:
            type2 = 'B'
            headindex[5] = 2
        elif globall<65536:
            type2 = 'H'
        #type1 = np.int16
            headindex[5] = 1
        else:
            type2 = 'i'
        l4 = output.tell()
        if indirect:
            output.write(struct.pack(type2*len(res2), *res2))
            l4 = output.tell()
            headindex[4] = l4-l3 # here is the size of the indirect mapping stored after minmax
            headindex[6] = len(res2)
        output.write(struct.pack(type1*len(offsets), *offsets))
    else:
        output.write(struct.pack('i',global_dict.index(minmax[0])))
        headindex[6] = 1
        l4 = output.tell()
        headindex[4] = l4-l3
        output.write(struct.pack('i',coldict[minmax[0]]))
    #output.write(array(type1,[coldict[y] for y in chunk]).tostring())
    l5 = output.tell()
    headindex[1] = l5-l4
    headindex[3] = len(chunk)
    output.seek(0)
    output.write(struct.pack(type, *headindex))
    f.write(output.getvalue())


def write_indirect(data,filen, ordering, row_group_offsets):
    f = open(filen,"w+b")
    f.write(MARKER)
    headindex = [0]*3
    l = len(data)
    headindex[0] = l
    headindex[1] = len(data.columns)
    type = 'i'*len(headindex)
    f.write(struct.pack(type, *headindex))
    nparts = max((l - 1) // row_group_offsets + 1, 1)
    chunksize = max(min((l - 1) // nparts + 1, l), 1)
    
    
    for col in data:
        l1 = f.tell()
        if ordering == 1:
            global_dict = sorted(data[col].unique())
            f.write(msgpack.dumps(global_dict))
        else:
            global_dict = data[col].unique()
            f.write(msgpack.dumps(list(global_dict)))
        l2 = f.tell()
        headindex[2] = l2-l1
        f.seek(4)
        f.write(struct.pack(type, *headindex))
        f.seek(l2)
        i=0
        j=1
        for part in range(nparts):
            chunk = data[col][chunksize*i:chunksize*j]
            globalindirect(f,global_dict,ordering,chunk)
            i+=1
            j+=1



import pyarrow as pa
import pyarrow.parquet as pq

def write_parquet(data,filen, row_group_offsets):
    if row_group_offsets == -1:
        table = pa.Table.from_pandas(data)
        pq.write_table(table, filen)
        return
    
    l = len(data)
    nparts = max((l - 1) // row_group_offsets + 1, 1)
    chunksize = max(min((l - 1) // nparts + 1, l), 1)
    
    ## write default no row groups
    #pq.write_table(table, 'example.parquet',compression={'10.1145/3025453.3025878': 'snappy', '10.1145/2317956.2318088': 'gzip'},use_dictionary=['10.1145/3025453.3025878','10.1145/2317956.2318088'])
    
    
    
    c = 0
    table = pa.Table.from_pandas(data)
    writer = pq.ParquetWriter(filen, table.schema, use_dictionary = True)
    for i in range(nparts):
        if i!=nparts:
            writer.write_table(table[c*chunksize:chunksize+c*chunksize])
            c+=1
        else:
            writer.write_table(table[:len(data)%chunksize])
    writer.close()
    





























