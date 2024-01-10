#!/usr/bin/env python

from mpi4py import MPI
import time
import cf
import os


def get_my_data(sid, ens) :

   comm = MPI.COMM_WORLD
   rank = comm.Get_rank()


   dkey = sid[-5:] + 'o_' + str(ens)
   print (dkey)

   files_type = { "1"   :  [dkey + "_mon__grid_T_",     'ncvar%sossheig'            ],
                  "2"   :  [dkey + "_mon__grid_T_",     'ncvar%zossq'               ],
                  "3"   :  [dkey + "_mon__grid_T_",     'ncvar%sowaflup'            ],
                  "4"   :  [dkey + "_mon__grid_T_",     'ncvar%sosafldo'            ],
                  "5"   :  [dkey + "_mon__grid_T_",     'ncvar%sohefldo'            ],
                  "6"   :  [dkey + "_mon__grid_T_",     'ncvar%somxl010'            ],
                  "7"   :  [dkey + "_mon__grid_T_",     'ncvar%votemper'            ],
                  "8"   :  [dkey + "_mon__grid_T_",     'ncvar%vosaline'            ],
                  "9"   :  [dkey + "_mon__grid_T_",     'ncvar%e3t'                 ],
                  "10"  :  [dkey + "_mon__grid_T_",     'ncvar%opottemptend'        ],
                  "11"  :  [dkey + "_mon__grid_T_",     'ncvar%osalttend'           ],
                  "12"  :  [dkey + "_mon__grid_T_",     'ncvar%soshfldo'            ],
                  "13"  :  [dkey + "_mon__grid_T_",     'ncvar%sorunoff'            ],
                  "14"  :  [dkey + "_mon__grid_T_",     'ncvar%sohflisf'            ],
                  "15"  :  [dkey + "_mon__grid_T_",     'ncvar%soqlatisf'           ],
                  "16"  :  [dkey + "_mon__grid_T_",     'ncvar%sowflisf'            ],
                  "17"  :  [dkey + "_mon__grid_T_",     'ncvar%berg_total_melt'     ],
                  "18"  :  [dkey + "_mon__grid_T_",     'ncvar%berg_total_heat_flux'],
                  "19"  :  [dkey + "_mon__grid_T_",     'ncvar%soicecov'            ],
                  "20"  :  [dkey + "_mon__grid_T_",     'ncvar%hfrainds'            ],
                  "21"  :  [dkey + "_mon__grid_T_",     'ncvar%hfevapds'            ],
                  "22"  :  [dkey + "_mon__grid_T_",     'ncvar%evap_ao_cea'         ],
                  "23"  :  [dkey + "_mon__grid_T_",     'ncvar%pr'                  ],
                  "24"  :  [dkey + "_mon__grid_T_",     'ncvar%snowpre'             ],
                  "25"  :  [dkey + "_mon__grid_T_",     'ncvar%snow_ao_cea'         ],
                  "26"  :  [dkey + "_mon__grid_T_",     'ncvar%snow_ai_cea'         ],
                  "27"  :  [dkey + "_mon__grid_T_",     'ncvar%botpres'             ],
                  "28"  :  [dkey + "_mon__grid_T_",     'ncvar%sshdyn'              ],
                  "29"  :  [dkey + "_mon__grid_T_",     'ncvar%ssh_ib'              ],
                  "30"  :  [dkey + "_mon__grid_T_",     'ncvar%qlw_oce'             ],
                  "31"  :  [dkey + "_mon__grid_T_",     'ncvar%qsb_oce'             ],
                  "32"  :  [dkey + "_mon__grid_T_",     'ncvar%qla_oce'             ],
                  "33"  :  [dkey + "_mon__grid_T_",     'ncvar%evap_oce'            ],
                  "34"  :  [dkey + "_mon__grid_T_",     'ncvar%qt_oce'              ],
                  "35"  :  [dkey + "_mon__grid_T_",     'ncvar%qsr_oce'             ],
                  "36"  :  [dkey + "_mon__grid_T_",     'ncvar%qns_oce'             ],
                  "37"  :  [dkey + "_mon__grid_T_",     'ncvar%qemp_oce'            ],
                  "38"  :  [dkey + "_mon__grid_T_",     'ncvar%taum_oce'            ],
                  "39"  :  [dkey + "_mon__grid_T_",     'ncvar%utau_oce'            ],
                  "40"  :  [dkey + "_mon__grid_T_",     'ncvar%vtau_oce'            ],
                  "41"  :  [dkey + "_mon__grid_U_",     'ncvar%e3u'                 ],
                  "42"  :  [dkey + "_mon__grid_U_",     'ncvar%vozocrtx'            ],
                  "43"  :  [dkey + "_mon__grid_U_",     'ncvar%ut'                  ],
                  "44"  :  [dkey + "_mon__grid_U_",     'ncvar%us'                  ],
                  "45"  :  [dkey + "_mon__grid_U_",     'ncvar%sozotaux'            ],
                  "46"  :  [dkey + "_mon__grid_V_",     'ncvar%e3v'                 ],
                  "47"  :  [dkey + "_mon__grid_V_",     'ncvar%vomecrty'            ],
                  "48"  :  [dkey + "_mon__grid_V_",     'ncvar%vt'                  ],
                  "49"  :  [dkey + "_mon__grid_V_",     'ncvar%vs'                  ],
                  "50"  :  [dkey + "_mon__grid_V_",     'ncvar%sometauy'            ],
                  "51"  :  [dkey + "_day__grid_T_",     'ncvar%tos'                 ],
                  "52"  :  [dkey + "_mon__diaptr_",     'ncvar%zotemglo'            ],
                  "53"  :  [dkey + "_mon__diaptr_",     'ncvar%zomsfatl'            ],
                  "54"  :  [dkey + "_mon__diaptr_",     'ncvar%zotematl'            ],
                  "55"  :  [dkey + "_mon__diaptr_",     'ncvar%zosrfatl'            ],
                  "56"  :  [dkey + "_mon__diaptr_",     'ncvar%zosalatl'            ],
                  "57"  :  [dkey + "_mon__diaptr_",     'ncvar%zomsfpac'            ],
                  "58"  :  [dkey + "_mon__diaptr_",     'ncvar%zotempac'            ],
                  "59"  :  [dkey + "_mon__diaptr_",     'ncvar%zosrfpac'            ],
                  "60"  :  [dkey + "_mon__diaptr_",     'ncvar%zosalpac'            ],
                  "61"  :  [dkey + "_mon__diaptr_",     'ncvar%zomsfind'            ],
                  "62"  :  [dkey + "_mon__diaptr_",     'ncvar%zotemind'            ],
                  "63"  :  [dkey + "_mon__diaptr_",     'ncvar%zosrfind'            ],
                  "64"  :  [dkey + "_mon__diaptr_",     'ncvar%zosalind'            ],
                  "65"  :  [dkey + "_mon__diaptr_",     'ncvar%sophtadv'            ],
                  "66"  :  [dkey + "_mon__diaptr_",     'ncvar%sopstadv'            ],
                  "67"  :  [dkey + "_mon__diaptr_",     'ncvar%sophtldf'            ],
                  "68"  :  [dkey + "_mon__diaptr_",     'ncvar%sopstldf'            ],
                  "69"  :  [dkey + "_mon__diaptr_",     'ncvar%sophtadv_atl'        ],
                  "70"  :  [dkey + "_mon__diaptr_",     'ncvar%sopstadv_atl'        ],
                  "71"  :  [dkey + "_mon__diaptr_",     'ncvar%sophtldf_atl'        ],
                  "72"  :  [dkey + "_mon__diaptr_",     'ncvar%sopstldf_atl'        ],
                  "73"  :  [dkey + "_mon__diaptr_",     'ncvar%zomsfglo'            ]}


# get list of keys
   key_list = list(files_type)
   print (' key_list:', key_list)


   print ('file  var :', files_type[key_list[rank]][0], files_type[key_list[rank]][1])

   years = range(1950, 1951)
   for yr in years :

       file_name = '/work/xfc/vol5/user_cache/rshatcher/canari/testing_downloads/' + sid + '/' + str(yr) + '*/' + files_type[key_list[rank]][0] + '*.nc'
       print ('rank: ', rank, 'file: ', file_name)

       aggregate_time = time.time()
       f = cf.read(file_name, aggregate={'ncvar_identities':True}, select=files_type[key_list[rank]][1], chunks=None)
       print (f)
       print('rank ', rank, ' year ', yr, "aggregate --- %s seconds ---" % (time.time() - aggregate_time))

       try:
           os.makedirs('/gws/nopw/j04/canari/shared/large-ensemble/HIST2/' + str(ens) + '/yearly/' + str(yr))
       except FileExistsError:
           pass

       out_file_name = '/gws/nopw/j04/canari/shared/large-ensemble/HIST2/' + str(ens) + '/yearly/' + str(yr) + '/' + files_type[key_list[rank]][0] +  files_type[key_list[rank]][1][6:] + '.nc'
       print ('rank: ', rank, ' year ', yr, 'out_file: ',  out_file_name)

       write_time = time.time()
       cf.write(f, out_file_name, compress=1)
       print('rank ', rank, ' year ', yr, ' finished writing ', "--- %s seconds ---" % (time.time() - write_time))



if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('suite', help="Suite ID to extract data from")
    parser.add_argument('ens_num', type=int, help="Ensemble number of suite")
    args = parser.parse_args()


    suite = args.suite
    ens   = args.ens_num

    print ('suite ', suite)
    print ('ens ',   ens)

    get_my_data(suite, ens)