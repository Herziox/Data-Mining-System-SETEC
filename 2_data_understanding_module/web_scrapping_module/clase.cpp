class MatEnergia {
	public:
		double **energia;
			int ele; //triangulos
			int tim; //Tiempo
			MatEnergia(){
				ele = 0;
				tim = 0;
				energia  = NULL;

			}

			void init(int e, int t){ //Elementos triangulares
				ele = e;
				tim = t;

                energia = new double* [ele];
                for (int i=0;i<ele;i++){
                    energia[i] = new double[tim];
                    for(int j=0;j<tim;j++){
                        energia[i][j] = 0.0;
                    }
                }
			};


};