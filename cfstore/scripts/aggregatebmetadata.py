import os

import cf


if __name__ == '__main__':
    outputdicts=[]

    for filename in os.listdir("{{fileinput}}"):
        #If should probably be a glob of some kind?
        if filename.endswith(".nca"):
            #Opens and empties matching files
            with open("{{homedir}}"+filename+"bmetadata.json","w") as writepath:
                print("")
            with open("{{homedir}}"+filename+"_variables_bmetadata.json","w") as writepath:
                print("")
            
    #Reads the fields from the file with cf
    #Alternatively cfdm can be used as such:
    #   cff = cfdm.read(filename)
    cff=cf.read("{{fileinput}}",ignore_read_error=True,fmt="NETCDF",aggregate={"relaxed_units":True,"relaxed_identities":True, "exclude":False,"concatenate":False},recursive=True, chunks=None)  

    print("Test",len(cff),[v.data.cfa_get_write() for v in cff])
    writepath="{{homedir}}/"+"tempfile.cfa"
    
    cf.write(cff,cfa={'strict',False},filename=writepath)
    