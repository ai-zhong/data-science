load('C:\Users\Zhou Qiao\Dropbox\Business\data\LatestData\etf.mat')
%%
%etfs.dts
%etfs.close


cube_master = f_joinCubes(etfs,bondetf);
cube_master = f_joinCubes(cube_master,cubeAssetClass);

%GLD,
%UUP,
%SPY,QQQ,IWM,EEM,EFA,
%IYR,
%USO,
%TLT'
names = {'GLD';...
        'USO';...
        'IYR';
        'UUP'
        'SPY';'QQQ';'IWM';'EEM';'EWZ';'FXI';...
        'AGG';'TLT'};
cube = cpcubesubsetbynames(cube_master,names);
cube = cpcubesubsetbydts(cube,cube.dts(all(~isnan(rets),2)));
%drop cols with missing data more than x%
nan_ratios = sum(isnan(cube.close))/size(cube.close,1)*100;
cube = cpcubesubsetbynames(cube_master,names);
cube = cpcubesubsetbynames(cube,cube.names(nan_ratios<=20));

rets = cube.close./lag(cube.close)-1;
rets(isnan(rets)) = 0;

corr(rets)

newcube = [];
newcube.dts = cube.dts;
for i = 1:length(cube.names)
    newcube.(cube.names{i}) = cube.close(:,i);
end

%%
tab = struct2table(newcube);
writetable(tab, 'ETFs.csv')
%bondetf.names
%equities, bonds, and gold via the ETFs SPY, AGG, and GLD

