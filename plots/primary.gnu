set xlabel "Time spent stalled (%)" 
set xlabel  offset character 0, 0.25, 0 font "" textcolor lt -1 norotate
set xrange [ 0.295000 : 0.0850000 ] reverse writeback
set ylabel "Average SSIM (dB)" 
set ylabel  offset character 1, 0, 0 font "" textcolor lt -1 rotate
set yrange [ 14.2000 : 16.3000 ] noreverse writeback
set border 3 front lt black linewidth 1.000 dashtype solid
set xtics border out scale 0.5,0.5 nomirror norotate  autojustify
set ytics border out scale 0.5,0.5 nomirror norotate  autojustify
set font "Times,10"
unset key
set object 1 polygon from 0.11099028,16.25177361 to 0.12257820,16.27144073 to 0.13473307,16.25177361 to 0.12257820,16.23219516 to 0.11099028,16.25177361
set object 1 noclip fc rgb '#000075' fillstyle solid lw 0 dt (0.000001,10000000)
set object 2 polygon from 0.21593152,16.03364113 to 0.24621307,16.05298187 to 0.28819990,16.03364113 to 0.24621307,16.01438613 to 0.21593152,16.03364113
set object 2 noclip fc rgb '#800000' fillstyle solid lw 0 dt (0.000001,10000000)
set object 3 polygon from 0.08786913,14.31117910 to 0.09977781,14.33186887 to 0.11226937,14.31117910 to 0.09977781,14.29058743 to 0.08786913,14.31117910
set object 3 noclip fc rgb '#911eb4' fillstyle solid lw 0 dt (0.000001,10000000)
set object 4 polygon from 0.15185357,15.89513302 to 0.16706356,15.91741020 to 0.18314708,15.89513302 to 0.16706356,15.87296952 to 0.15185357,15.89513302
set object 4 noclip fc rgb '#469990' fillstyle solid lw 0 dt (0.000001,10000000)
set object 5 polygon from 0.16879002,14.93994462 to 0.18525316,14.95746001 to 0.20314905,14.93994462 to 0.18525316,14.92249957 to 0.16879002,14.93994462
set object 5 noclip fc rgb '#9a6324' fillstyle solid lw 0 dt (0.000001,10000000)
plot '-' with labels
0.12257820 16.28144073 "Tetra"
0.24621307 16.06298187 "MPC-HM"
0.09977781 14.34186887 "RobustMPC-HM"
0.16706356 15.9274102 "Pensieve"
0.18525316 14.96746001 "BBA"
e
